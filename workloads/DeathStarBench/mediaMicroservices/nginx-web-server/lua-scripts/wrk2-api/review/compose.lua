local _M = {}

local function _StrIsEmpty(s)
  return s == nil or s == ''
end

local function _UploadUserId(req_id, post, carrier, service_config)
  local GenericObjectPool = require "GenericObjectPool"
  local UserServiceClient = require 'media_service_UserService'
  local user_client = GenericObjectPool:connection(
      UserServiceClient, service_config["user-service"]["addr"],
      service_config["user-service"]["port"],
      service_config["user-service"]["http_path"])
  local status, err = pcall(user_client.UploadUserWithUsername, user_client,
      req_id, post.username, carrier)
  if not status then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Upload user_id failed: " .. err.message)
    ngx.log(ngx.ERR, "Upload user_id failed: " .. err.message)
    ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
  end
  GenericObjectPool:returnConnection(user_client)
end

local function _UploadText(req_id, post, carrier, service_config)
  local GenericObjectPool = require "GenericObjectPool"
  local TextServiceClient = require 'media_service_TextService'
  local text_client = GenericObjectPool:connection(
      TextServiceClient, service_config["text-service"]["addr"],
      service_config["text-service"]["port"],
      service_config["text-service"]["http_path"])
  local status, err = pcall(text_client.UploadText, text_client,
      req_id, post.text, carrier)
  if not status then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Upload text failed: " .. err.message)
    ngx.log(ngx.ERR, "Upload text failed: " .. err.message)
    ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
  end
  GenericObjectPool:returnConnection(text_client)
end

local function _UploadMovieId(req_id, post, carrier, service_config)
  local GenericObjectPool = require "GenericObjectPool"
  local MovieIdServiceClient = require 'media_service_MovieIdService'
  local movie_id_client = GenericObjectPool:connection(
      MovieIdServiceClient, service_config["movie-id-service"]["addr"],
      service_config["movie-id-service"]["port"],
      service_config["movie-id-service"]["http_path"])
  local status, err = pcall(movie_id_client.UploadMovieId, movie_id_client,
      req_id, post.title, tonumber(post.rating), carrier)
  if not status then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Upload movie_id failed: " .. err.message)
    ngx.log(ngx.ERR, "Upload movie_id failed: " .. err.message)
    ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
  end
  GenericObjectPool:returnConnection(movie_id_client)
end

local function _UploadUniqueId(req_id, carrier, service_config)
  local GenericObjectPool = require "GenericObjectPool"
  local UniqueIdServiceClient = require 'media_service_UniqueIdService'
  local unique_id_client = GenericObjectPool:connection(
      UniqueIdServiceClient, service_config["unique-id-service"]["addr"],
      service_config["unique-id-service"]["port"],
      service_config["unique-id-service"]["http_path"])
  local status, err = pcall(unique_id_client.UploadUniqueId, unique_id_client,
      req_id, carrier)
  if not status then
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.say("Upload unique_id failed: " .. err.message)
    ngx.log(ngx.ERR, "Upload unique_id failed: " .. err.message)
    ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
  end
  GenericObjectPool:returnConnection(unique_id_client)
end

function _M.ComposeReview()
  local bridge_tracer = require "opentracing_bridge_tracer"
  local ngx = ngx
  local cjson = require("cjson")
  local service_config = cjson.decode(ngx.shared.config:get("service"))

  local req_id = tonumber(string.sub(ngx.var.request_id, 0, 15), 16)
  local tracer = bridge_tracer.new_from_global()
  local parent_span_context = tracer:binary_extract(ngx.var.opentracing_binary_context)
  local span = tracer:start_span("ComposeReview", {["references"] = {{"child_of", parent_span_context}}})
  local carrier = {}
  tracer:text_map_inject(span:context(), carrier)

  ngx.req.read_body()
  local post = ngx.req.get_post_args()

  if (_StrIsEmpty(post.title) or _StrIsEmpty(post.text) or
      _StrIsEmpty(post.username) or _StrIsEmpty(post.password) or
      _StrIsEmpty(post.rating)) then
    ngx.status = ngx.HTTP_BAD_REQUEST
    ngx.say("Incomplete arguments")
    ngx.log(ngx.ERR, "Incomplete arguments")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
  end

  local threads = {
    ngx.thread.spawn(_UploadUserId, req_id, post, carrier, service_config),
    ngx.thread.spawn(_UploadMovieId, req_id, post, carrier, service_config),
    ngx.thread.spawn(_UploadText, req_id, post, carrier, service_config),
    ngx.thread.spawn(_UploadUniqueId, req_id, carrier, service_config)
  }

  local status = ngx.HTTP_OK
  for i = 1, #threads do
    local ok, res = ngx.thread.wait(threads[i])
    if not ok then
      status = ngx.HTTP_INTERNAL_SERVER_ERROR
    end
  end
  span:finish()
  ngx.exit(status)
  
end

return _M