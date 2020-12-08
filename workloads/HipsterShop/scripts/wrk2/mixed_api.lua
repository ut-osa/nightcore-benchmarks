function init(rand_seed)
  math.randomseed(rand_seed)
end

local products = {
  '0PUK6V6EV0',
  '1YMWWN1N4O',
  '2ZYFJ3GM2N',
  '66VCHSJNUP',
  '6E92ZMYYFZ',
  '9SIQT8TOJO',
  'L9ECAV7KIM',
  'LS4PSXUNUM',
  'OLJCESPC7Z'
}

local charset = {'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'a', 's',
  'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm', 'Q',
  'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H',
  'J', 'K', 'L', 'Z', 'X', 'C', 'V', 'B', 'N', 'M', '1', '2', '3', '4', '5',
  '6', '7', '8', '9', '0'}

local function random_str(length)
  if length > 0 then
    return random_str(length - 1) .. charset[math.random(1, #charset)]
  else
    return ""
  end
end

local function home()
  local session_id = tostring(math.random(1000))
  local method = "GET"
  local path = "http://localhost:8080/function/home?session_id=" .. session_id
  local headers = {}
  return wrk.format(method, path, headers, nil)
end

local function product()
  local session_id = tostring(math.random(1000))
  -- local product = products[math.random(#products)]
  local product = string.format("%04x", math.random(1000))
  local method = "GET"
  local path = "/function/product?id=" .. product .. "&session_id=" .. session_id
  local headers = {}
  return wrk.format(method, path, headers, nil)
end

local function view_cart()
  local session_id = tostring(math.random(1000))
  local method = "GET"
  local path = "/function/viewCart?session_id=" .. session_id
  local headers = {}
  return wrk.format(method, path, headers, nil)
end

local function add_cart()
  local session_id = tostring(math.random(1000))
  -- local product = products[math.random(#products)]
  local product = string.format("%04x", math.random(1000))
  local quantity = math.random(1, 10)
  local method = "POST"
  local path = "/function/addToCart"
  local body = '{"product_id":"' .. product .. '",' ..
    '"quantity":"' .. tostring(quantity) .. '",' ..
    '"session_id":"' .. session_id .. '"}'
  local headers = {}
  headers["Content-Type"] = "application/json"
  return wrk.format(method, path, headers, body)
end

local function checkout()
  local session_id = tostring(math.random(1000))
  local method = "POST"
  local path = "/function/checkout"
  local body = '{"email":"' .. random_str(5) .. "@example.com" .. '",' ..
    '"street_address":"' .. random_str(10) .. '",' ..
    '"zip_code":"' .. '99999' .. '",' ..
    '"city":"' .. random_str(5) .. '",' ..
    '"state":"' .. 'CA' .. '",' ..
    '"country":"' .. 'US' .. '",' ..
    '"credit_card_number":"' .. '4432-8015-6152-0454' .. '",' ..
    '"credit_card_expiration_month":"' .. '1' .. '",' ..
    '"credit_card_expiration_year":"' .. '2039' .. '",' ..
    '"credit_card_cvv":"' .. '672' .. '",' ..
    '"session_id":"' .. session_id .. '"}'
  local headers = {}
  headers["Content-Type"] = "application/json"
  return wrk.format(method, path, headers, body)
end

request = function()
  local home_ratio      = 0.3
  local product_ratio   = 0.3
  local view_cart_ratio = 0.2
  local add_cart_ratio  = 0.1
  local checkout_ratio  = 0.1

  local coin = math.random()
  if coin < home_ratio then
    return home()
  elseif coin < home_ratio + product_ratio then
    return product()
  elseif coin < home_ratio + product_ratio + view_cart_ratio then
    return view_cart()
  elseif coin < home_ratio + product_ratio + view_cart_ratio + add_cart_ratio then
    return add_cart()
  else
    return checkout()
  end
end
