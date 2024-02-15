#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "synchronizer.grpc.pb.h"
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

namespace fs = std::filesystem;

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
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
//using csce438::ServerList;
using csce438::SynchService;
using csce438::AllUsers;
using csce438::TLFL;
using csce438::AllSyncInfo;
using csce438::PathAndData;

int synchID = 1;
std::string directory;
std::string slaveDirectory;
std::vector<std::string> get_lines_from_file(std::string);
void run_synchronizer(std::string,std::string,std::string,int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
bool file_contains_user(std::string, std::string);

struct Post {
    Post(std::string& ts, std::string& un, std::string& m): timestamp(ts), username(un), msg(m) {};
    std::string timestamp;
    std::string username;
    std::string msg;
};

void AppendToTimeline(std::string& filename, std::string& data) {
    std::ofstream outfile;
    outfile.open(filename, std::ios_base::app); // append instead of overwrite
    outfile << data;
}

std::string ConvertPostToData(Post& post) {
  std::string str = "T ";
  str += post.timestamp;
  str += '\n';
  str += "U ";
  str += post.username;
  str += '\n';
  str += "W ";
  str += post.msg;
  str += '\n';
  str += '\n';
  return str;
}

class SynchServiceImpl final : public SynchService::Service {
    Status GetAllUsers(ServerContext* context, const Confirmation* confirmation, AllUsers* allusers) override{
        std::vector<std::string> list = get_all_users_func(synchID);
        //package list
        for(auto s:list){
            allusers->add_users(s);
        }

        //return list
        return Status::OK;
    }

    Status GetTLFL(ServerContext* context, const ID* id, TLFL* tlfl){
        //std::cout<<"Got GetTLFL"<<std::endl;
        int clientID = id->id();

        std::vector<std::string> tl = get_tl_or_fl(synchID, clientID, true);
        std::vector<std::string> fl = get_tl_or_fl(synchID, clientID, false);

        //now populate TLFL tl and fl for return
        for(auto s:tl){
            tlfl->add_tl(s);
        }
        for(auto s:fl){
            tlfl->add_fl(s);
        }
        tlfl->set_status(true); 

        return Status::OK;
    }

    Status ResynchServer(ServerContext* context, const ServerInfo* serverinfo, Confirmation* c){
        std::cout<<serverinfo->type()<<"("<<serverinfo->serverid()<<") just restarted and needs to be resynched with counterpart"<<std::endl;
        std::string backupServerType;
        //copy files in master directory to slave distory
        // YOUR CODE HERE
        fs::path source = directory;
        fs::path destination = slaveDirectory;

        for (const auto& entry : fs::directory_iterator(source)) {
            const fs::path current_path = entry.path();
            const fs::path new_path = destination / current_path.filename();
            fs::copy_file(current_path, new_path, fs::copy_options::overwrite_existing);
        }


        return Status::OK;
    }
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){
  //localhost = 127.0.0.1
  std::string server_address("localhost:"+port_no);
  SynchServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}



int main(int argc, char** argv) {
  
  int opt = 0;
  std::string coordIP;
  std::string coordPort;
  std::string port = "3029";

  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1){
    switch(opt) {
      case 'h':
          coordIP = optarg;
          break;
      case 'k':
          coordPort = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      case 'i':
          synchID = std::stoi(optarg);
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }
  directory = "./master" + std::to_string(synchID);
  slaveDirectory = "./slave" + std::to_string(synchID);

  RunServer(coordIP, coordPort, port, synchID);
  return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){
    //setup coordinator stub
    //std::cout<<"synchronizer stub"<<std::endl;
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
    //std::cout<<"MADE STUB"<<std::endl;

    ServerInfo msg;
    Confirmation c;
    grpc::ClientContext context;

    msg.set_serverid(synchID);
    // msg.set_hostname("127.0.0.1");
    msg.set_hostname(coordIP);
    msg.set_port(port);
    msg.set_type("synchronizer");

    //send init heartbeat
    PathAndData pathAndData;
    ServerInfo* info = new ServerInfo(msg);
    pathAndData.set_path(std::to_string(synchID));
    pathAndData.set_allocated_data(info);

    Status status = coord_stub_->Create(&context, pathAndData, &c);
    if(!c.status()) {
        // create znode failed
        log(ERROR, "failed to create znode");
    } else {
        log(INFO, "znode created successfully");
    }

    //TODO: begin synchronization process
    while(true){
        //change this to 30 eventually
        sleep(5);
        //synch all users file 
        //get list of all followers(synchronizer)
        ClientContext context;
        Empty empty;
        AllSyncInfo allSyncInfo;

        coord_stub_->GetAllSynchronizer(&context, empty, &allSyncInfo);

        // YOUR CODE HERE
        //set up stub
        std::set<std::string> userSet;
        uint n = allSyncInfo.hostnames().size();
        for(int i = 0; i < n; i++) {
            //set up stub
            // syncStub
            std::string target_str = allSyncInfo.hostnames(i) + ":" + allSyncInfo.ports(i);
            std::unique_ptr<SynchService::Stub> sync_stub_;
            sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
            
            ClientContext syncContext;
            Confirmation confirmation;
            AllUsers allusers;
            sync_stub_->GetAllUsers(&syncContext, confirmation, &allusers);
            // //send each a GetAllUsers request
            // synStub->getAllUsers
            // //aggregate users into a list
            uint nUser = allusers.users_size();
            for(int i = 0; i < nUser; i++) userSet.insert(allusers.users(i));
        }
        //sort list and remove duplicates (get all users from each synchronizer)
        // YOUR CODE HERE
        std::string master_users_file = directory + "/all_users";
        std::ofstream outfile;
        outfile.open(master_users_file, std::ofstream::trunc);
        for(auto& ele : userSet) {
            outfile << ele+'\n';
        }
        std::string slave_users_file = slaveDirectory + "/all_users";
        std::ofstream slaveOutfile;
        slaveOutfile.open(slave_users_file, std::ofstream::trunc);
        for(auto& ele : userSet) {
            slaveOutfile << ele+'\n';
        }
        //for all the found users
        //if user not managed by current synch
        //sync follower_list of managed user
        for(auto& user : userSet) {
            int userID = stoi(user);
            if(synchID != ((userID - 1)%3)+1) { 
                ClientContext context;
                ID id;
                id.set_id(userID);
                ServerInfo serverInfo;
                Status status = coord_stub_->GetFollowerSyncer(&context, id, &serverInfo);

                std::string target_str = serverInfo.hostname() + ":" + serverInfo.port();

                std::unique_ptr<SynchService::Stub> sync_stub_;
                sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
                ClientContext syncContext;
                TLFL tlfl;
                sync_stub_->GetTLFL(&syncContext, id, &tlfl);
                for(auto& fl : tlfl.fl()) {
                    int flID = stoi(fl);
                    if(synchID == ((flID - 1)%3)+1) {  
                        //append to follower list
                        std::string followerList = directory + "/" + fl + "_follower_list";
                        std::string slaveFollowerList = slaveDirectory + "/" + fl + "_follower_list";

                        if(!file_contains_user(followerList, user)){
                            std::ofstream outfile;
                            outfile.open(followerList, std::ios_base::app); // append instead of overwrite
                            outfile << user+'\n';

                            std::ofstream slaveOutfile;
                            slaveOutfile.open(slaveFollowerList, std::ios_base::app); // append instead of overwrite
                            slaveOutfile << user+'\n';
                        }
                    } 
                }
            }
        }

        // YOUR CODE HERE

    //force update managed users from newly synced users
        //for all users
        // for(auto i : aggregated_users){
        for(auto& user : userSet) {
            int userID = stoi(user);
            //get currently managed users
            //if user IS managed by current synch
            if(synchID == ((userID - 1)%3)+1) {
                //read their follower lists (follow list)
                std::vector<std::string> following = get_tl_or_fl(synchID, userID, false);
                for(auto& fl : following) {
                    int flID = stoi(fl);
                    //for followed users that are not managed on cluster (if user that current user follows is managed by other synchronizer)
                    if(synchID != ((flID - 1)%3)+1) {
                        //read followed users cached timeline (read c2's cached timeline (post of c2))
                        ClientContext context;
                        ID id;
                        id.set_id(flID);
                        ServerInfo serverInfo;
                        Status status = coord_stub_->GetFollowerSyncer(&context, id, &serverInfo);

                        std::string target_str = serverInfo.hostname() + ":" + serverInfo.port();
                        std::unique_ptr<SynchService::Stub> sync_stub_;
                        sync_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));
                        ClientContext syncContext;
                        TLFL tlfl;
                        sync_stub_->GetTLFL(&syncContext, id, &tlfl);

                        //parse flID's post from timeline
                        std::vector<Post> posts;
                        int n = tlfl.tl().size();
                        for(int i = 0; i < n; i+=3) {
                            std::string timestampStr = tlfl.tl(i).substr(2);
                            std::string username = tlfl.tl(i+1).substr(2);
                            std::string msg = tlfl.tl(i+2).substr(2);
                            //std::cout<<timestampStr<<" "<<username<<" "<<msg<<std::endl;
                            if(username == fl) {
                                posts.emplace_back(timestampStr, username, msg);
                            }
                        }
                        // get c1's timeline using get_tl_or_fl
                        std::vector<std::string> userTimeline = get_tl_or_fl(synchID, userID, true);
                        //check if posts are in the managed tl (check if c1's timeline contains c2's post)
                        int nUser = userTimeline.size();
                        int postIdx = 0;
                        for(int i = 0; i < nUser; i+=3) {
                            std::string timestampStr = userTimeline[i].substr(2);
                            std::string username = userTimeline[i+1].substr(2);
                            std::string msg = userTimeline[i+2].substr(2);
                            if(username == fl && timestampStr == posts[postIdx].timestamp && msg == posts[postIdx].msg) {
                                //posts[postIdx] is in timeline
                                postIdx++;
                            }
                        }
                        //add post to tl of managed user  (add c2's post to c1's timeline (1_timeline))
                        //put posts[postIdx:] to c1's timeline
                        int nPost = posts.size();
                        std::string timeline = directory + "/" + user + "_timeline";
                        std::string slaveTimeline = slaveDirectory + "/" + user + "_timeline";
                        for(int i = postIdx; i < nPost; i++) {
                            std::string data = ConvertPostToData(posts[i]);
                            AppendToTimeline(timeline, data);
                            AppendToTimeline(slaveTimeline, data);
                        }
                        //send post data to client stream? or
                        //parse timeline from server?
                        
                    }
                }
            }
        }
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename){
  std::vector<std::string> users;
  std::string user;
  std::ifstream file; 
  file.open(filename);
  if(file.peek() == std::ifstream::traits_type::eof()){
    //return empty vector if empty file
    //std::cout<<"returned empty vector bc empty file"<<std::endl;
    file.close();
    return users;
  }
  while(file){
    getline(file,user);

    if(!user.empty())
      users.push_back(user);
  } 

  file.close();

  //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
  /*for(int i = 0; i<users.size(); i++){
    std::cout<<users[i]<<std::endl;
  }*/ 

  return users;
}

bool file_contains_user(std::string filename, std::string user){
    std::vector<std::string> users;
    //check username is valid
    users = get_lines_from_file(filename);
    for(int i = 0; i<users.size(); i++){
      //std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
      if(user == users[i]){
        //std::cout<<"found"<<std::endl;
        return true;
      }
    }
    //std::cout<<"not found"<<std::endl;
    return false;
}

std::vector<std::string> get_all_users_func(int synchID){
    //read all_users file master and client for correct serverID
    std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
    //take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if(master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl){
    std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    if(tl){
        master_fn.append("_timeline");
        slave_fn.append("_timeline");
    }else{
        master_fn.append("_follow_list");
        slave_fn.append("_follow_list");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if(m.size()>=s.size()){
        return m;
    }else{
        return s;
    }

}
