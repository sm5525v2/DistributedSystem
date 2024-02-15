#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>
#include <string>
#include <stdlib.h>
#include <unistd.h>

#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 



using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::ClientContext;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::PathAndData;


std::unique_ptr<CoordService::Stub> coord_stub_;

ServerInfo serverInfo;

void CreateZnode(int synchronizerID, std::string& hostname, std::string& port) {
  //setting serverInfo

  ClientContext context;
  PathAndData pathAndData;
  serverInfo.set_serverid(synchronizerID);
  serverInfo.set_hostname(hostname);
  serverInfo.set_port(port);
  serverInfo.set_type("synchronizer");

  ServerInfo* info = new ServerInfo(serverInfo);
  //set clusterID as synchronizerID
  pathAndData.set_path(std::to_string(synchronizerID));
  pathAndData.set_allocated_data(info);
  Confirmation confirmation;

  Status status = coord_stub_->Create(&context, pathAndData, &confirmation);
  if(!confirmation.status()) {
    // create znode failed
    log(ERROR, "failed to create synchronizer znode");
  }
  log(INFO, "synchronizer znode created successfully");
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string coord_port = "9000";
  std::string coord_hostname = "0.0.0.0";
  int synchronizerID = 0;

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:k:h:i:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      case 'k':
          coord_port = optarg;break;
      case 'h':
          coord_hostname = optarg;break;
      case 'i':
          synchronizerID = atoi(optarg);break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  
  // creating stub for CoordService
  std::string coord_login_info = coord_hostname + ":" + coord_port;
  coord_stub_ = CoordService::NewStub(grpc::CreateChannel(coord_login_info,
      grpc::InsecureChannelCredentials()));

  CreateZnode(synchronizerID, coord_hostname, port);
//   std::thread hb{Heartbeat};
//   RunSynchronizer(coord_hostname, port, synchronizerID); //coord_hostname == hostname for server

  return 0;
}
