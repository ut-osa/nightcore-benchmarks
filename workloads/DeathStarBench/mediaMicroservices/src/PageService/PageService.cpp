#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <signal.h>

#include "../utils.h"
#include "../ThriftSeverEventHandler.h"
#include "PageHandler.h"

using json = nlohmann::json;
using apache::thrift::server::TNonblockingServer;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TNonblockingServerSocket;
using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::concurrency::ThreadManager;
using apache::thrift::concurrency::ThreadFactory;
using apache::thrift::concurrency::PlatformThreadFactory;
using namespace media_service;

void sigintHandler(int sig) {
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "cast-info-service");

  json config_json;
  if (load_config(&config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = 9090;
  std::string cast_info_addr = config_json["cast-info-service"]["addr"];
  int cast_info_port = config_json["cast-info-service"]["port"];
  std::string cast_info_http_path = config_json["cast-info-service"]["http_path"];
  std::string movie_review_addr = config_json["movie-review-service"]["addr"];
  int movie_review_port = config_json["movie-review-service"]["port"];
  std::string movie_review_http_path = config_json["movie-review-service"]["http_path"];
  std::string movie_info_addr = config_json["movie-info-service"]["addr"];
  int movie_info_port = config_json["movie-info-service"]["port"];
  std::string movie_info_http_path = config_json["movie-info-servic"]["http_path"];
  std::string plot_addr = config_json["plot-service"]["addr"];
  int plot_port = config_json["plot-service"]["port"];
  std::string plot_http_path = config_json["plot-service"]["http_path"];

  ClientPool<ThriftClient<MovieInfoServiceClient>>
      movie_info_client_pool("movie-info-client", movie_info_addr,
                             movie_info_port, 0, 128, 1000, movie_info_http_path,
                             "PageService", "MovieInfoService");
  ClientPool<ThriftClient<CastInfoServiceClient>>
      cast_info_client_pool("cast-info-client", cast_info_addr,
                            cast_info_port, 0, 128, 1000, cast_info_http_path,
                            "PageService", "CastInfoService");
  ClientPool<ThriftClient<MovieReviewServiceClient>>
      movie_review_client_pool("movie-review-client", movie_review_addr,
                               movie_review_port, 0, 128, 1000, movie_review_http_path,
                            "PageService", "MovieReviewService");
  ClientPool<ThriftClient<PlotServiceClient>>
      plot_client_pool("plot-client", plot_addr, plot_port, 0, 128, 1000, plot_http_path,
                       "PageService", "PlotService");

  int server_threadpool_size = config_json["page-service"]["server_threadpool_size"];
  std::cout << "Use " << server_threadpool_size << " threads for the page-service server" << std::endl;
  auto thread_manager = ThreadManager::newSimpleThreadManager(server_threadpool_size, 0);
  thread_manager->threadFactory(std::shared_ptr<ThreadFactory>(
      new apache::thrift::concurrency::PlatformThreadFactory(false)));
  TNonblockingServer server(
      std::make_shared<PageServiceProcessor>(
          std::make_shared<PageHandler>(
              &movie_review_client_pool,
              &movie_info_client_pool,
              &cast_info_client_pool,
              &plot_client_pool)),
      std::make_shared<TBinaryProtocolFactory>(),
      std::make_shared<TNonblockingServerSocket>("0.0.0.0", port),
      thread_manager
  );
  thread_manager->start();
  int server_num_io_threads = config_json["page-service"]["server_num_io_threads"];
  std::cout << "Use " << server_num_io_threads << " IO threads for the page-service server" << std::endl;
  server.setNumIOThreads(server_num_io_threads);

  server.setServerEventHandler(std::make_shared<ThriftServerEventHandler>("PageService"));
  std::cout << "Starting the page-service server ..." << std::endl;
  server.serve();
}