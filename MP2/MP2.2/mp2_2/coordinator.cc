#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "synchronizer.grpc.pb.h"
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using google::protobuf::Empty;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::ClientContext;
using grpc::Channel;
using csce438::Request;
using csce438::Reply;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::PathAndData;
using csce438::AllSyncInfo;
using csce438::SNSService;
using csce438::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe
std::mutex v_mutex;

// std::vector<zNode> cluster1;
// std::vector<zNode> cluster2;
// std::vector<zNode> cluster3;

std::unordered_map<int, std::unordered_map<int, zNode*>> clusterMap;
std::unordered_map<int, zNode*> synchronizerMap;

std::unordered_map<std::string, int> clusterIDMapper;

std::unordered_map<int, int> masterMap; //key = clusterID, value = serverID;
std::unordered_map<int, zNode*> slaveMap;

int serverCnt = 0;


//func declarations
int findServer(std::vector<zNode> v, int id);
std::time_t getTimeNow();
void checkHeartbeat();


//bool ServerStruct::isActive(){
bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}

class CoordServiceImpl final : public CoordService::Service {

  
  Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    // std::cout<<"Got Heartbeat! "<<serverinfo->type()<<"("<<serverinfo->serverid()<<")"<<std::endl;
    log(INFO, "Got Heartbeat! " + serverinfo->type() + "(" + std::to_string(serverinfo->serverid()) + ")");
    // Your code here
    // find server(znode) using serverinfo
    // if found set last heartbeat
    // if not found, create znode and  set last heartbeat?
    std::string key = serverinfo->hostname() + ":" +  serverinfo->port();
    if(clusterIDMapper.count(key)) {
      int clusterID = clusterIDMapper[key];
      auto server = clusterMap[clusterID][serverinfo->serverid()];
      server->last_heartbeat = getTimeNow();
    } else {
      log(ERROR, "Couldn't find serverinfo");
    }
    confirmation->set_status(true);
    return Status::OK;
  }
  
  //function returns the server information for requested client id
  //this function assumes there are always 3 clusters and has math
  //hardcoded to represent this.
  Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    //std::cout<<"Got GetServer for clientID: "<<id->id()<<std::endl;
    log(INFO, "Got GetServer for clientID: " + std::to_string(id->id()));
    int clusterID = ((id->id() - 1)%3)+1;
    // int serverID = 1;
    
    // Your code here
    // If server is active, return serverinfo
    //get server
    zNode* server = nullptr;
    bool findServer = false;
    //server exist?
    for(auto& entry : clusterMap[clusterID]) {
      server = entry.second;
      //find active server
      if(server->isActive() && masterMap[clusterID] == server->serverID) {
        serverinfo->set_serverid(server->serverID);
        serverinfo->set_hostname(server->hostname);
        serverinfo->set_port(server->port);
        serverinfo->set_type(server->type);
        findServer = true;
        break;
      }
    }
    if(!findServer) {
      log(ERROR, "couldn't find active server in cluster"+clusterID);
    }
    return Status::OK;
  }

  Status GetSlave(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    int clusterID = id->id();
    // int serverID = 1;
    
    // Your code here
    // If server is active, return serverinfo
    //get server
    zNode* server = slaveMap[clusterID];

    if(!server) {
      return Status::CANCELLED;
    } else {
      serverinfo->set_serverid(server->serverID);
      serverinfo->set_hostname(server->hostname);
      serverinfo->set_port(server->port);
      serverinfo->set_type(server->type);
      return Status::OK;
    }
  }
  
  Status GetFollowerSyncer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    //log(INFO, "Got GetFollowerSyncer for clientID: " + std::to_string(id->id()));
    int clusterID = ((id->id() - 1)%3)+1;
    // int serverID = 1;
    
    // Your code here
    // If server is active, return serverinfo
    //get server
    zNode* server = synchronizerMap[clusterID];

    if(!server) {
      log(ERROR, "couldn't find synchronizer in cluster"+clusterID);
    }
    if(!server->isActive()) {
      log(ERROR, "Synchronizer is inactive"+clusterID);
    }
    serverinfo->set_serverid(server->serverID);
    serverinfo->set_hostname(server->hostname);
    serverinfo->set_port(server->port);
    serverinfo->set_type(server->type);
    return Status::OK;
  }

  Status GetAllSynchronizer(ServerContext* context, const Empty* request, AllSyncInfo* response) override{
    log(INFO, "Got GetAllSynchronizer");
    for(auto [key, server]: synchronizerMap) {
      response->add_hostnames(server->hostname);
      response->add_ports(server->port);
    }
    return Status::OK;
  }

  Status Create(ServerContext* context, const PathAndData* pathAndData, Confirmation* confirmation) {
    ServerInfo serverinfo = pathAndData->data();
    std::string key = serverinfo.hostname() + ":" + serverinfo.port();
    log(INFO, "Got Create for " + serverinfo.type() + " " + key);
    if(serverinfo.type() == "server") {
      if(clusterIDMapper.count(key)) {
        //duplicated znode data 
        log(WARNING, "duplicate znode data");
        int clusterID = std::stoi(pathAndData->path());
        zNode* server = clusterMap[clusterID][serverinfo.serverid()];
        std::string directory = "";
        if(!masterMap.count(clusterID)) {
          //set master
          std::cout<<"set exist z node as master"<<std::endl;
          masterMap[clusterID] = serverinfo.serverid();
          log(INFO, "Master of cluster"+std::to_string(clusterID)+" is server"+std::to_string(serverinfo.serverid()));
        } else {
          //set slave
          std::cout<<"set exist znode as slave"<<std::endl;
          slaveMap[clusterID] = server;

          //sync from master

          zNode* synchonizer = synchronizerMap[clusterID];
          std::string target_str = synchonizer->hostname + ":" + synchonizer->port;
          // ServerInfo syncServerInfo;
          // serverInfo.set_serverid(serverID);
          // serverInfo.set_hostname(hostname);
          // serverInfo.set_port(port);
          // serverInfo.set_type("server");
          Confirmation c;
          auto sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
          ClientContext syncContext;
          sync_stub_->ResynchServer(&syncContext, serverinfo, &c);
          std::cout<<"Resynched Server"<<std::endl;
        }

      } else {
        zNode* newServer = new zNode{serverinfo.serverid(), serverinfo.hostname(), 
            serverinfo.port(), serverinfo.type(), getTimeNow(), false};
        int clusterID = std::stoi(pathAndData->path());
        clusterMap[clusterID][serverinfo.serverid()] = newServer;
        clusterIDMapper[key] = clusterID;
        std::string directory = "";
        if(!masterMap.count(clusterID)) {
          //set master
          masterMap[clusterID] = serverinfo.serverid();
          log(INFO, "Master of cluster"+std::to_string(clusterID)+" is server"+std::to_string(serverinfo.serverid()));
          directory = "master" + std::to_string(clusterID);
        } else {
          //set slave
          slaveMap[clusterID] = newServer;
          directory = "slave" + std::to_string(clusterID);
        }
        std::filesystem::create_directory(directory);
        log(INFO, "Created znode for server " + key);
      }
    } else if(serverinfo.type() == "synchronizer") {
      if(synchronizerMap.count(serverinfo.serverid())) {
        //duplicated znode data 
        log(WARNING, "duplicate znode data");
      } else {
        zNode* newServer = new zNode{serverinfo.serverid(), serverinfo.hostname(), 
            serverinfo.port(), serverinfo.type(), getTimeNow(), false};
        int clusterID = std::stoi(pathAndData->path());
        synchronizerMap[clusterID] = newServer;
        // std::string directory = "server_" + std::to_string(clusterID) + "_" + std::to_string(serverinfo.serverid());
        // std::filesystem::create_directory(directory);
        log(INFO, "Created znode for synchronizer " + key);
      } 
    }
    confirmation->set_status(true);
    return Status::OK;
  }

};

void RunServer(std::string port_no){
  //start thread to check heartbeats
  std::thread hb(checkHeartbeat);
  //localhost = 127.0.0.1
  std::string server_address("localhost:"+port_no);
  CoordServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> coordinator(builder.BuildAndStart());
  std::cout<<"Coordinator listening on "<<server_address<<std::endl;
  log(INFO, "Coordinator listening on " + server_address);

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  coordinator->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("coordinator-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Coordinator starting...");

  RunServer(port);
  return 0;
}



void checkHeartbeat(){
  while(true){
    //check servers for heartbeat > 10
    //if true turn missed heartbeat = true
    // Your code below


    for(auto& cluster : clusterMap){
      auto cCluster = cluster.second; //copy
      for(auto& s : cCluster) {
        if(difftime(getTimeNow(),s.second->last_heartbeat)>10){
          if(!s.second->missed_heartbeat){
            s.second->missed_heartbeat = true;
            s.second->last_heartbeat = getTimeNow();
          }else{
            //missed heartbeat before
            //server is down now
            int clusterID = cluster.first;
            int serverID = s.first;
            log(WARNING, "Cluster"+std::to_string(clusterID)+" server"+std::to_string(serverID)+" is down");
            //cluster.second.erase(s.first);
            if(masterMap[clusterID] == serverID) {
              //set new master
              for(auto& candidate : cluster.second) {
                if(candidate.second->isActive()) {
                  masterMap[clusterID] = candidate.first;
                  log(WARNING, "Master of cluster"+std::to_string(clusterID)+" is now server"+std::to_string(candidate.first));
                  slaveMap.erase(clusterID);
                  zNode* newMaster = candidate.second;
                  //set master to server
                  std::string server_login_info = newMaster->hostname + ":" + newMaster->port;
                  std::cout<<"new master address "<<server_login_info<<std::endl;
                  std::unique_ptr<SNSService::Stub> stub = SNSService::NewStub(grpc::CreateChannel(server_login_info,
                                        grpc::InsecureChannelCredentials()));

                  ClientContext context;
                  Reply reply;
                  Request request;
                  Status status = stub->SetMaster(&context, request, &reply);
                  break;
                }
              }
            }
          }
        }
      }
    }
    
    sleep(5);
  }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}
