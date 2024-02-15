/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

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


struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

std::unique_ptr<CoordService::Stub> coord_stub_;

ServerInfo serverInfo;

Client* GetClient(std::string username) {
    for(Client* client: client_db) {
      if(client->username == username) return client;
    }
    // no client exist
    return nullptr;
}

std::string ConvertMessageToDataRecord(Message& message) {
  std::string str = "T ";
  str += google::protobuf::util::TimeUtil::ToString(message.timestamp());
  str += '\n';
  str += "U ";
  str += message.username();
  str += '\n';
  str += "W ";
  str += message.msg();
  str += '\n';
  str += '\n';
  return str;
}

Message MakeMessage(const std::string& timestampStr, const std::string& username, const std::string& msg) {
    Message m;
    // Create a Timestamp instance
    Timestamp* timestamp = new Timestamp();

    // set timestampStr info to timestamp
    google::protobuf::util::TimeUtil::FromString(timestampStr, timestamp);
    m.set_allocated_timestamp(timestamp);
    m.set_username(username);
    m.set_msg(msg);
    return m;
}

void AppendToTimeline(std::string& filename, std::string& data) {
    std::ofstream outfile;
    outfile.open(filename, std::ios_base::app); // append instead of overwrite
    outfile << data;
}

void ParseMessagesFromTimeline(std::string& filename, std::deque<Message>* messages) {
    std::ifstream timeline;
    timeline.open(filename, std::ios_base::app);
    if (timeline.is_open()) { 
        std::string line;
        // Read data from the file object and put it into a string.
        while (getline(timeline, line)) { 
            //parse timestamp
            std::string timestampStr = line.substr(2);
            getline(timeline, line);
            //parse username
            std::string username = line.substr(2);
            getline(timeline, line);
            //parse post content
            std::string msg = line.substr(2);
            Message newMsg = MakeMessage(timestampStr, username, msg);
            line.substr(2);
            messages->push_front(newMsg);
            //parse empty line
            getline(timeline, line);

            //remain message queue size smaller than 20
            if(messages->size() > 20) messages->pop_back();
        }
        
        // Close the file object.
        timeline.close(); 
    }
}

class SNSServiceImpl final : public SNSService::Service {

  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    /*********
    YOUR CODE HERE
    **********/
    std::string user = request->username();
    log(INFO, "Got List command for clientID: " + user);
    std::cout<<"Got List command for clientID: "<<user<<std::endl;
    Client* client = GetClient(user);
    for(Client* c : client_db) {
      list_reply->add_all_users(c->username);
    }
    for(Client* f : client->client_followers) {
      list_reply->add_followers(f->username);
    }
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
    std::string user1 = request->username(); 
    std::string user2 = request->arguments(0);
    Client* c1 = GetClient(user1);
    Client* c2 = GetClient(user2);
    if(c2 && c1 != c2) {
        // c1 already following c2
        if(find(c1->client_following.begin(), c1->client_following.end(), c2) != c1->client_following.end()) {
            //std::cout<<c1->username<<" already followed "<<c2->username<<std::endl;
            reply->set_msg("Already followed");
        } else {
            c1->client_following.push_back(c2);
            c2->client_followers.push_back(c1);
            //std::cout<<c1->username<<" now following "<<c2->username<<std::endl;
            reply->set_msg("Follow successful");
        }
    } else {
      //c2 doesn't exist or c1 is equal to c2
      //std::cout<<"Invalid username: "<<user2<<std::endl;
      reply->set_msg("Invalid username");
    }
    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
    std::string user1 = request->username(); 
    std::string user2 = request->arguments(0);
    Client* c1 = GetClient(user1);
    Client* c2 = GetClient(user2);
    if(c2 && (c1 != c2)) {
        auto iter = find(c1->client_following.begin(), c1->client_following.end(), c2);
        if(iter != c1->client_following.end()) {
            c1->client_following.erase(iter);
            c2->client_followers.erase(find(c2->client_followers.begin(), c2->client_followers.end(), c1));
            //std::cout<<c1->username<<" unfollow "<<c2->username<<std::endl;
            reply->set_msg("Unfollow successful");
        } else {
            std::cout<<c1->username<<" does not following "<<c2->username<<std::endl;
            reply->set_msg("Not following");
        }
    } else {
      //c2 doesn't exist or c1 == c2
      //std::cout<<"Invalid username: "<<user2<<std::endl;
      reply->set_msg("Invalid username");
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
    std::string user = request->username(); 
    if(!GetClient(user)) {
        // create new Client
        Client* newClient = new Client();
        newClient->username = user;
        client_db.push_back(newClient);
        std::string filename = directory + "/" + user+".txt";

        //check existing file first
        // std::ifstream timeline;
        // timeline.open(filename, std::ios_base::app);
        // if (!timeline.is_open()) { 
        //     // create new timeline file
        //     std::ofstream outfile (filename);
        //     outfile.close();
        // }

        //create new timeline file always
        std::ofstream outfile (filename);
        outfile.close();
    }
    return Status::OK;
  }

    // RPC Login
  Status CheckStatus(ServerContext* context, const Request* request, Reply* reply) override {
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
      
    /*********
    YOUR CODE HERE
    **********/
    Message m;
    bool isFirst = true;
    while(stream->Read(&m)){
        std::string user = m.username();
        Client* c = GetClient(user);
        std::string filename =  directory + "/" + user + ".txt";
        if(isFirst) {
            // handle initialization to show exist timeline
            std::deque<Message> messages;
            //set client's stream
            c->stream = stream;
            ParseMessagesFromTimeline(filename, &messages);
            for(Message& msg : messages) {
                c->stream->Write(msg);
            }
            isFirst = false;
        } else {
            //convert message to store data format
            std::string data = ConvertMessageToDataRecord(m);
            AppendToTimeline(filename, data); 
            //show posts to follower's timeline
            for(Client* f : c->client_followers) {
                if(f->stream) {
                    f->stream->Write(m);
                }
                // append data to the follower's timeline
                std::string fFilename = directory + "/" + f->username + ".txt";
                AppendToTimeline(fFilename, data); 
            }
        }
    }
    
    return Status::OK;
  }

  std::string directory;
public:
  void setDirectory(std::string& input) { directory = input;}
};

void CreateZnode(int clusterID, int serverID, std::string& hostname, std::string& port) {
  //setting serverInfo

  ClientContext context;
  PathAndData pathAndData;
  serverInfo.set_serverid(serverID);
  serverInfo.set_hostname(hostname);
  serverInfo.set_port(port);
  serverInfo.set_type("server");

  ServerInfo* info = new ServerInfo(serverInfo);
  pathAndData.set_path(std::to_string(clusterID));
  pathAndData.set_allocated_data(info);
  Confirmation confirmation;

  Status status = coord_stub_->Create(&context, pathAndData, &confirmation);
  if(!confirmation.status()) {
    // create znode failed
    log(ERROR, "failed to create znode");
  }
  log(INFO, "znode created successfully");
}

void Heartbeat() {
  while(true){
    ClientContext context;
    Confirmation confirmation;
    Status status = coord_stub_->Heartbeat(&context, serverInfo, &confirmation);
    sleep(5);
  }
}

void RunServer(std::string& hostname, std::string& port_no, int clusterID, int serverID) {

  std::string server_address = hostname + ":" + port_no;
  SNSServiceImpl service;
  std::string directory = "server_" + std::to_string(clusterID) + "_" + std::to_string(serverID);

  service.setDirectory(directory);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout<<"Server listening on "<<server_address<<std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string coord_port = "9000";
  std::string coord_hostname = "0.0.0.0";
  int clusterID = 0;
  int serverID = 0;

  
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:k:h:c:s:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      case 'k':
          coord_port = optarg;break;
      case 'h':
          coord_hostname = optarg;break;
      case 'c':
          clusterID = atoi(optarg);break;
      case 's':
          serverID = atoi(optarg);break;
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

  CreateZnode(clusterID, serverID, coord_hostname, port);
  std::thread hb{Heartbeat};
  RunServer(coord_hostname, port, clusterID, serverID); //coord_hostname == hostname for server

  return 0;
}
