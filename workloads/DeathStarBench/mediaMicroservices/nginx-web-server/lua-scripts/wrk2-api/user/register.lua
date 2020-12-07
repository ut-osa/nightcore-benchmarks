local _M = {}

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

function _M.RegisterUser()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local GenericObjectPool = require "GenericObjectPool"
  local UserServiceClient = require 'media_service_UserService'
  local ngx = ngx
  local cjson = require("cjson")
  local service_config = cjson.decode(ngx.shared.config:get("service"))

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  local tracer = bridge_tracer.new_from_global()
  local parent_span_context = tracer:binary_extract(ngx.var.opentracing_binary_context)
  local span = tracer:start_span("RegisterUser", {["references"] = {{"child_of", parent_span_context}}})
  local carrier = {}
  tracer:text_map_inject(span:context(), carrier)

  ngx.req.read_body()
  local post = ngx.req.get_post_args()

  if (_StrIsEmpty(post.first_name) or _StrIsEmpty(post.last_name) or
      _StrIsEmpty(post.username) or _StrIsEmpty(post.password)) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  local client = GenericObjectPool:connection(
    UserServiceClient, service_config["user-service"]["addr"],
    service_config["user-service"]["port"],
    service_config["user-service"]["http_path"])

  client:RegisterUser(req_id, post.first_name, post.last_name,
      post.username, post.password, carrier)
  GenericObjectPool:returnConnection(client)

  span:finish()
end

return _M