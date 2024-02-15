#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include <google/protobuf/util/time_util.h>

#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
//using csce438::PathAndData;
//using csce438::Status;
//using csce438::Path;


void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& port)
    :coord_hostname(hname), username(uname), coord_port(port) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  std::string server_hostname; //server
  std::string server_port; //server
  std::string coord_hostname; //coordinator
  std::string coord_port; //coordinator
  std::string username;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;

  std::unique_ptr<CoordService::Stub> coord_stub_;
  
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);

  ServerInfo GetServerInfo();
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------
    
///////////////////////////////////////////////////////////
// YOUR CODE HERE
//////////////////////////////////////////////////////////
    ServerInfo serverInfo = GetServerInfo();
    server_hostname = serverInfo.hostname();
    server_port = serverInfo.port();

    std::string server_login_info = server_hostname + ":" + server_port;

    stub_ = SNSService::NewStub(grpc::CreateChannel(server_login_info,
                          grpc::InsecureChannelCredentials()));
    IReply reply = Login();
    if(!reply.comm_status == IStatus::SUCCESS) {
      return -1;
    }
    return 1;
}

ServerInfo Client::GetServerInfo() {
    std::string coord_login_info = coord_hostname + ":" + coord_port;
    coord_stub_ = CoordService::NewStub(grpc::CreateChannel(coord_login_info,
                          grpc::InsecureChannelCredentials()));
    // get server info
    ClientContext context;
    ID id;
    id.set_id(stoi(username));
    ServerInfo serverInfo;
    
    Status status = coord_stub_->GetServer(&context, id, &serverInfo);
    if(status.ok()) {
      log(INFO, "got server info");
    } else {
      log(ERROR, "fail to get server info");
    }
    return serverInfo;
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;
    
    /*********
    YOUR CODE HERE
    **********/
    std::string cmd, arg;
    if(input.find(" ") != std::string::npos) {
        cmd = input.substr(0,input.find(" "));
        arg = input.substr(input.find(" ") + 1);
    } else {
        cmd = input;
    }

    if("FOLLOW" == cmd){
        ire = Follow(arg);
    } else if("UNFOLLOW" == cmd){
        ire = UnFollow(arg);
    } else if("LIST" == cmd){
        ire = List();
    } else if("TIMELINE" == cmd){
        //time line will be handled in processTimeline() method
        ClientContext context;
        Reply reply;
        Request request;
        request.set_username(username);
        Status status = stub_->CheckStatus(&context, request, &reply);
        if(status.ok()) {
          //go to timeline mode if channel is alive
          ire.grpc_status = Status::OK;
          ire.comm_status = IStatus::SUCCESS;
        }
    }

    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

    IReply ire;

    /*********
    YOUR CODE HERE
    **********/
    ClientContext context;
    ListReply listReply;
    Request request;
    request.set_username(username);
    Status status = stub_->List(&context, request, &listReply);
    ire.grpc_status = status;
    if(status.ok()) {
        for(auto& user : listReply.all_users()) ire.all_users.push_back(user);
        for(auto& follower : listReply.followers()) ire.followers.push_back(follower);
        ire.comm_status = IStatus::SUCCESS;
    } else {
        // if(connectTo() == 1) List();
        // else {
          ire.comm_status = IStatus::FAILURE_UNKNOWN;
        // }
    }
    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
      
    /***
    YOUR CODE HERE
    ***/
    ClientContext context;
    Reply reply;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Status status = stub_->Follow(&context, request, &reply);
    if(reply.msg() == "Already followed"){
      ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
    } else if(reply.msg() == "Follow successful") {
      ire.comm_status = IStatus::SUCCESS;
    } else if(reply.msg() == "Invalid username") {
      ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
    }
    ire.grpc_status = status;
    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    /***
    YOUR CODE HERE
    ***/
    ClientContext context;
    Reply reply;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Status status = stub_->UnFollow(&context, request, &reply);
    if(reply.msg() == "Not following"){
      ire.comm_status = IStatus::FAILURE_NOT_A_FOLLOWER;
    } else if(reply.msg() == "Invalid username") {
      ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
    } else if(reply.msg() == "Unfollow successful") {
      ire.comm_status = IStatus::SUCCESS;
    }
    ire.grpc_status = status;
    return ire;
}

// Login Command  
IReply Client::Login() {

    IReply ire;
  
    /***
     YOUR CODE HERE
    ***/
    ClientContext context;
    Reply reply;
    Request request;
    request.set_username(username);
    Status status = stub_->Login(&context, request, &reply);
    if(status.ok()) {
        if (reply.msg() == "Already exist") {
            ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;  
        } else {
            ire.comm_status = IStatus::SUCCESS;
        }
    } else {
        ire.comm_status = IStatus::FAILURE_INVALID;
    }
    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions 
    // in client.cc file for both getting and displaying messages 
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
  
    /***
    YOUR CODE HERE
    ***/

    ClientContext context;
    //change RouteNote to Message
    std::shared_ptr<ClientReaderWriter<Message, Message> > stream(
        stub_->Timeline(&context));

    //send initial msg to show existing timeline
    Message initialMsg;
    initialMsg.set_username(username);
    initialMsg.set_msg("initial");
    stream->Write(initialMsg);

    std::thread writer_thread([stream, this]() {
        while(1) {
            std::string input;
            std::getline(std::cin, input);
            Message msg = MakeMessage(this->username, input);
            stream->Write(msg);
            //stream->WritesDone();
        }
    });

    std::thread reader_thread([stream]() {
        Message server_msg;
        while (stream->Read(&server_msg)) {
          //std::string msgStr = getPostMessage();
          std::time_t time = google::protobuf::util::TimeUtil::TimestampToTimeT(server_msg.timestamp());
          displayPostMessage(server_msg.username(), server_msg.msg(), time);
          std::cout << std::endl;
        }
    });
    writer_thread.join();
    reader_thread.join();
    Status status = stream->Finish();
    if (!status.ok()) {
      log(ERROR, "TimeLine rpc failed.");
      // std::cout << "TimeLine rpc failed." << std::endl;
    }

}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";
  std::string coordPort = "9000";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'k':
      coordPort = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
  std::string log_file_name = std::string("client-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Client starting...");

  Client myc(hostname, username, coordPort);
  myc.run();
  
  return 0;
}
