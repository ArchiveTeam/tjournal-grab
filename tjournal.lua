dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil
local item_site = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}
local got_pages = {}
local allowed_urls = {}
local primary_url = nil
local secondary_url = nil

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    target[item] = true
  end
end

find_item = function(url)
  site, post = string.match(url, "^https?://([^/]+)/([0-9]+)$")
  if site and post and (site == "tjournal.ru" or site == "vc.ru" or site == "dtf.ru") then
    item_type = "post"
    item_site = site
    item_value = post
    item_name_new = item_type .. ":" .. item_site .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      got_pages = {}
      primary_url = url
      ids[item_value] = true
      abortgrab = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
    return nil
  end
  if string.match(url, "^https?://leonardo%.osnova%.io/") then
    item_type = "url"
    item_site = nil
    item_value = url
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      abortgrab = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if string.match(url, "^https?://leonardo%.osnova%.io/") then
    discovered_items["url:" .. url] = true
    return false
  end

  if allowed_urls[url] then
    return true
  end

  if string.match(url, "^https?://[^/]-([^%./]+%.[^%./]+)/") == item_site then
    for s in string.gmatch(url, "([0-9]+)") do
      if ids[s] then
        return true
      end
    end
  end

  if string.match(url, "^https?://([^/]+)/") ~= item_site
    and not string.match(url, "^https?://leonardo%.osnova%.io") then
    discover_item(discovered_outlinks, url)
    return false
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if parent["url"] == primary_url then
    return true
  end

  if processed(url) then
    return false
  end

  if allowed(url) then
    return true
  end

  if discovered_items["url:" .. url] then
    return false
  end

  if urlpos["link_refresh_p"] ~= 0 or urlpos["link_inline_p"] ~= 0 then
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    if string.match(newurl, "^https?://[^/]+/comments/loading/[0-9]+") then
      return false
    end
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  if allowed(url) and status_code < 300
    and string.match(url, "^https?://[^/]-([^%./]+%.[^%./]+)/") == item_site then
    if string.match(file, "^%s*<") then
      html = file
    else
      html = read_file(file)
    end
    check(secondary_url .. "?comments")
    if not got_pages["hit"] then
      table.insert(urls, {
        url="https://tjournal.ru/hit/" .. item_value,
        body_data="mode=raw",
        method="POST",
        headers={
          ["X-This-Is-CSRF"]="THIS IS SPARTA!"
        }
      })
      got_pages["hit"] = true
    end
    if string.match(url, "/comments/loading/")
      and string.match(html, '^{"') then
      local json = JSON:decode(html)
      if json["data"]["remaining_count"] ~= 0 or json["rc"] ~= 200 then
        abort_item()
        return {}
      end
      for _, data in pairs(json["data"]["items"]) do
        wget.callbacks.get_urls(data["html"], url)
      end
      return urls
    end
    if string.match(url, "^https?://[^/]+/comments/[0-9]+/get$")
      and string.match(html, '^{"') then
      local json = JSON:decode(html)
      if json["rc"] ~= 200 then
        abort_item()
        return {}
      end
      for _, data in pairs(json["data"]) do
        wget.callbacks.get_urls(data["html"], url)
      end
      return urls
    end
    for comments_data in string.gmatch(html, '<div[^>]+(class="comment__load%-more[^>]+)"') do
      data_ids = string.match(comments_data, 'data%-ids="([^"]+)"')
      data_with_subtree = string.match(comments_data, 'data%-with%-subtree="([^"]+)"')
      if not data_ids
        or not data_with_subtree
        or data_with_subtree ~= "1"
        or not string.match(data_ids, "^[,0-9]+$") then
        abort_item()
      end
      data_ids = string.gsub(data_ids, ",", "_")
      if not got_pages[data_ids] then
        table.insert(urls, {
          url="https://" .. item_site .. "/comments/loading/" .. item_value,
          body_data="ids=" .. data_ids .. "&with_subtree=true&mode=raw",
          method="POST",
          headers={
            ["X-This-Is-CSRF"]="THIS IS SPARTA!"
          }
        })
        got_pages[data_ids] = true
      end
    end
    for attribute, suffixes in pairs({
      ["data%-image%-src"]={
        "/",
        "/-/preview/{}/-/format/webp/",
        "/-/scale_crop/64x64/-/format/webp/",
        "/-/scale_crop/108x108/-/format/webp/",
        "/-/scale_crop/172x172/-/format/webp/",
        "/-/scale_crop/200x200/-/format/webp/",
        "/-/scale_crop/1024x1024/-/format/webp/",
      },
      ["data%-video%-thumbnail"]={
        "/",
        "/-/format/webp/-/preview/{}/",
      },
      ["data%-video%-mp4"]={
        "/",
        "/-/format/mp4/",
      }
    }) do
      for newurl in string.gmatch(html, attribute .. '="([^"]+)"') do
        local base = string.match(newurl, "^(https?://leonardo%.osnova%.io/[a-f0-9%-]+)")
        if base then
          for _, suffix in pairs(suffixes) do
            for i=300,1100,100 do
              local newurl_ = base .. string.gsub(suffix, "{}", tostring(i))
              allowed_urls[newurl_] = true
              check(newurl_)
            end
          end
        end
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  find_item(url["url"])
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  find_item(url["url"])

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
    if url["url"] == primary_url then
      secondary_url = newloc
    end
  end

  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if (status_code == 0 or status_code >= 400)
    and status_code ~= 404 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 4
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.ABORT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 4
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["urls-ajsy1kcax4kmzsu"] = discovered_outlinks,
    ["tjournal-p93yjmrvti21rwg"] = discovered_items
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

