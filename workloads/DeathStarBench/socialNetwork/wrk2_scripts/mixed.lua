--require "socket"
--math.randomseed(socket.gettime()*1000)
math.random(); math.random(); math.random()

local charset = {'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'a', 's',
  'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm', 'Q',
  'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H',
  'J', 'K', 'L', 'Z', 'X', 'C', 'V', 'B', 'N', 'M', '1', '2', '3', '4', '5',
  '6', '7', '8', '9', '0'}

local decset = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}

local function stringRandom(length)
  if length > 0 then
    return stringRandom(length - 1) .. charset[math.random(1, #charset)]
  else
    return ""
  end
end

local function decRandom(length)
  if length > 0 then
    return decRandom(length - 1) .. decset[math.random(1, #decset)]
  else
    return ""
  end
end

local function composePost()
  local user_index = math.random(1, 962)
  local username = "username_" .. tostring(user_index)
  local user_id = tostring(user_index)
  local text = stringRandom(64)
  local num_user_mentions = math.random(0, 2)
  local num_urls = math.random(0, 2)
  local num_media = math.random(0, 2)
  local media_ids = '['
  local media_types = '['

  for i = 0, num_user_mentions, 1 do
    local user_mention_id
    while (true) do
      user_mention_id = math.random(1, 962)
      if user_index ~= user_mention_id then
        break
      end
    end
    text = text .. " @username_" .. tostring(user_mention_id)
  end

  for i = 0, num_urls, 1 do
    text = text .. " http://" .. stringRandom(64)
  end

  for i = 0, num_media, 1 do
    local media_id = decRandom(18)
    media_ids = media_ids .. "\"" .. media_id .. "\","
    media_types = media_types .. "\"png\","
  end

  media_ids = media_ids:sub(1, #media_ids - 1) .. "]"
  media_types = media_types:sub(1, #media_types - 1) .. "]"

  local method = "POST"
  local path = "http://localhost:8080/wrk2-api/post/compose"
  local headers = {}
  local body
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  if num_media then
    body   = "username=" .. username .. "&user_id=" .. user_id ..
        "&text=" .. text .. "&media_ids=" .. media_ids ..
        "&media_types=" .. media_types .. "&post_type=0"
  else
    body   = "username=" .. username .. "&user_id=" .. user_id ..
        "&text=" .. text .. "&media_ids=" .. "&post_type=0"
  end

--   if req_id ~= "" then
--     headers["Req-Id"] = req_id
--   end

  return wrk.format(method, path, headers, body)
end

local function readHomeTimeline()
  local user_id = tostring(math.random(1, 962))
  local start = tostring(math.random(0, 100))
  local stop = tostring(start + 1)

  local args = "user_id=" .. user_id .. "&start=" .. start .. "&stop=" .. stop
  local method = "GET"
  local headers = {}
  -- headers["Content-Type"] = "application/x-www-form-urlencoded"
  local path = "http://localhost:8080/wrk2-api/home-timeline/read?" .. args

  return wrk.format(method, path, headers, nil)
end

local function readUserTimeline()
  local user_id = tostring(math.random(1, 962))
  local start = tostring(math.random(0, 100))
  local stop = tostring(start + 1)

  local args = "user_id=" .. user_id .. "&start=" .. start .. "&stop=" .. stop
  local method = "GET"
  local headers = {}
  -- headers["Content-Type"] = "application/x-www-form-urlencoded"
  local path = "http://localhost:8080/wrk2-api/user-timeline/read?" .. args

  return wrk.format(method, path, headers, nil)
end

local function userFollow()
  local user_id = tostring(math.random(1, 962))
  local followee_id = tostring(math.random(1, 962))

  local body = "user_id=" .. user_id .. "&followee_id=" .. followee_id
  local method = "POST"
  local headers = {}
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  local path = "http://localhost:8080/wrk2-api/user/follow"

  return wrk.format(method, path, headers, body)
end

request = function()
  local user_timeline_ratio = 0.4
  local home_timeline_ratio = 0.25
  local compose_post_ratio  = 0.30
  local follow_ratio        = 0.05

  local coin = math.random()
  if coin < user_timeline_ratio then
    return readUserTimeline()
  elseif coin < user_timeline_ratio + home_timeline_ratio then
    return readHomeTimeline()
  elseif coin < user_timeline_ratio + home_timeline_ratio + compose_post_ratio then
    return composePost()
  else 
    return userFollow()
  end
end

function init(rand_seed)
  math.randomseed(rand_seed)
end
