-- require "socket"
-- local time = socket.gettime()*1000
-- math.randomseed(time)
math.random(); math.random(); math.random()

request = function(req_id)
  local user_id = tostring(math.random(1, 962))
  local start = tostring(math.random(0, 100))
  local stop = tostring(start + 1)

  local args = "user_id=" .. user_id .. "&start=" .. start .. "&stop=" .. stop
  local method = "GET"
  local headers = {}
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  local path = "http://localhost:8080/wrk2-api/user-timeline/read?" .. args
  if req_id ~= "" then
    headers["Req-Id"] = req_id
  end
  return wrk.format(method, path, headers, nil)

end
