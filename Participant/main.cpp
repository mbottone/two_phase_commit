//
//  main.cpp
//  Participant
//
//  Name - Michael Bottone
//  Advanced Distributed Systems - Fall 2015
//

#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <sstream>
#include <pthread.h>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <queue>
#include <time.h>
#include <signal.h>

using namespace std;

// ** Global Types and Properties

enum ActionType
{
    ROLLBACK = 0,
    COMMIT = 1
};

enum VoteStatus
{
    VOTE_NO = 0,
    VOTE_YES = 1
};

enum SystemStatus
{
    NORMAL = 0,
    RECOVERY = 1,
    FAILED = 2,
    FINISHED = 3
};

SystemStatus system_status;

struct Packet
{
    int socket;
    int * data;
    int length;
    int timestamp;
    
    void sendPacket()
    {
        send(socket, data, length, 0);
    }
    
    static Packet createFromRawData(int * data, int socket, int length)
    {
        Packet p;
        
        p.socket = socket;
        p.data = data;
        p.length = length;
        p.timestamp = data[0];
        
        return p;
    }
    
    static Packet createVotePacket(VoteStatus vote, int socket, int requestId)
    {
        Packet p;
        
        p.socket = socket;
        p.data = new int[3];
        p.length = 3 * sizeof(int);
        
        time_t currentTime;
        time(&currentTime);
        p.data[0] = (int)currentTime;
        p.data[1] = requestId;
        p.data[2] = vote;
        
        return p;
    }
    
    static Packet createAckPacket(int socket, int requestId)
    {
        Packet p;
        
        p.socket = socket;
        p.length = 2 * sizeof(int);
        p.data = new int[2];
        
        time_t currentTime;
        time(&currentTime);
        p.data[0] = (int)currentTime;
        
        p.data[1] = requestId;
        
        return p;
    }
};

struct Response
{
    int requestId = 0;
    bool isRequest;
    int socket;
    int tickets;
    vector<int> dates;
    ActionType action;
    
    static Response createFromPacket(Packet p)
    {
        Response res;
        
        res.socket = p.socket;
        res.requestId = p.data[1];
        if (p.length > 12)
        {
            res.isRequest = true;
            res.tickets = p.data[2];
            int dateCount = p.data[3];
            
            for (int i = 0;i < dateCount;i ++)
            {
                int date = p.data[4 + i];
                res.dates.push_back(date);
            }
        }
        else
        {
            res.isRequest = false;
            res.action = ActionType(p.data[2]);
        }
        
        return res;
    }
};

// ** Global Functions **

// Split string by a delimeter into a vector of tokens
vector<string> split(string fullString, char delimiter)
{
    vector<string> splits;
    stringstream stream(fullString);
    string token;
    
    while(getline(stream, token, delimiter))
    {
        splits.push_back(token);
    }
    
    return splits;
}

class CommunicationSubstrate
{
private:
    
    // ** Class Parameters **
    
    pthread_t bufferThread;
    pthread_t recieveThread;
    
    queue<Packet> inputBuffer;
    queue<Packet> outputBuffer;
    
    sockaddr_in serverSocketInfo;
    sockaddr_in coordinatorInfo;
    
    int coordinatorSocket;
    int messageSocket;
    
    string participantAddress;
    
    queue<Response> responseBuffer;
    
    // ** Private Functions **
    
    void populateIPAddress(sockaddr_in * address, string addressInfo)
    {
        address->sin_family = AF_INET;
        vector<string> addressParts = split(addressInfo, ':');
        address->sin_port = htons(stoi(addressParts[1]));
        inet_pton(AF_INET, addressParts[0].c_str(), &address->sin_addr);
    }
    
    void connectToCoordinator(bool fresh)
    {
        if (fresh)
        {
            populateIPAddress(&serverSocketInfo, participantAddress);
            if (::bind(coordinatorSocket, (sockaddr *)&serverSocketInfo, sizeof(serverSocketInfo)) == -1)
            {
                cout << "Bind error " << errno << endl;
                exit(1);
            }
            
            if (listen(coordinatorSocket, 0) == -1)
            {
                cout << "Listen error" << errno << endl;
                exit(1);
            }
            
            cout << "Bound on address " << participantAddress << endl;
        }
        cout << "Waiting for connection from coordinator..." << endl;
        
        socklen_t size;
        messageSocket = accept(coordinatorSocket, (sockaddr *)&coordinatorInfo, &size);
        if (messageSocket == -1)
        {
            cout << "Error accepting connection" << endl;
            exit(1);
        }
        
        cout << "Coordinator connected." << endl;
    }
    
    // Function to start thread D
    static void *messageRecieveThreadCaller(void * context)
    {
        return ((CommunicationSubstrate *)context)->recieveMessages(NULL);
    }
    
    void recieveDataFromSocket(int socket, int * buffer)
    {
        size_t bytesRecieved = recv(socket, buffer, 64, 0);
        if (bytesRecieved > 0)
        {
            if (bytesRecieved == sizeof(int))
            {
                cout << "Finished packet recieved" << endl;
                stopSubstrate();
                exit(0);
            }
            Packet packet = Packet::createFromRawData(buffer, socket, (int) bytesRecieved);
            if (system_status == NORMAL)
            {
                inputBuffer.push(packet);
            }
        }
    }
    
    // Threaded function to recieve messages
    void * recieveMessages(void *)
    {
        int * buffer = new int[64];
        
        while (system_status != FINISHED)
        {
            recieveDataFromSocket(messageSocket, buffer);
        }
        
        pthread_exit(NULL);
    }
    
    // Function to start thread C
    static void *substrateThreadCaller(void * context)
    {
        return ((CommunicationSubstrate *)context)->processBuffers(NULL);
    }
    
    // Threaded function to process buffers
    void * processBuffers(void *)
    {
        while (system_status != FINISHED)
        {
            if (!outputBuffer.empty())
            {
                Packet p = outputBuffer.front();
                p.sendPacket();
                outputBuffer.pop();
            }
            
            if (!inputBuffer.empty())
            {
                Packet p = inputBuffer.front();
                Response res = Response::createFromPacket(p);
                responseBuffer.push(res);
                inputBuffer.pop();
            }
        }
        
        pthread_exit(NULL);
    }
    
    void startSubstrate()
    {
        cout << "Starting communication substrate..." << endl;
        
        if (int s = pthread_create(&bufferThread, NULL, &CommunicationSubstrate::substrateThreadCaller, this))
        {
            cout << "Error creating buffer thread. Code - " << s << endl;
            exit(1);
        }
        
        if (int s = pthread_create(&recieveThread, NULL, &CommunicationSubstrate::messageRecieveThreadCaller, this))
        {
            cout << "Error creating message thread. Code - " << s << endl;
            exit(1);
        }
        
        cout << "Communication substrate started." << endl;
    }
    
public:
    
    // ** Public Functions **
    
    CommunicationSubstrate(string socketAddress)
    {
        coordinatorSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        participantAddress = socketAddress;
        connectToCoordinator(true);
        startSubstrate();
    }
    
    Response waitForResponse()
    {
        cout << "Waiting for response..." << endl;
        Response r;
        
        time_t startTime;
        time_t currentTime;
        time(&startTime);
        
        while (system_status == NORMAL)
        {
            time(&currentTime);
            if (currentTime - startTime > 10)
            {
                cout << "Timeout..." << endl;
                return Response();
            }
            
            if (!responseBuffer.empty())
            {
                r = responseBuffer.front();
                responseBuffer.pop();
                return r;
            }
        }
        
        return Response();
    }
    
    void sendVote(VoteStatus vote, int requestId)
    {
        cout << "Sending " << (vote == VOTE_YES ? "yes vote for " : "no vote for ") << requestId << endl;
        
        Packet votePacket = Packet::createVotePacket(vote, messageSocket, requestId);
        outputBuffer.push(votePacket);
    }
    
    void sendAck(int requestId)
    {
        cout << "Sending acknowledgement for id " << requestId << endl;
        
        Packet ackPacket = Packet::createAckPacket(messageSocket, requestId);
        outputBuffer.push(ackPacket);
    }
    
    void stopSubstrate()
    {
        close(coordinatorSocket);
        close(messageSocket);
    }
    
    void failSystem()
    {
        outputBuffer = queue<Packet>();
        inputBuffer = queue<Packet>();
        responseBuffer = queue<Response>();
        
        cout << "Communication Substrate failed." << endl;
    }
    
    void reconnect()
    {
        connectToCoordinator(false);
    }
};

class Participant
{
private:
    
    // ** Class Parameters **
    
    string configFile;
    string myAddress;
    
    vector<int> bookingData;
    
    pthread_t processThread;
    
    CommunicationSubstrate * comm;
    
    Response commitStorage;
    
    ofstream outputFile;
    ofstream logfile;
    
    string outputName;
    
    // ** Private Functions **
    
    // Read lines from a given file
    vector<string> readFile(string filename)
    {
        ifstream readFile (filename);
        string line;
        vector<string> lines;
        if (readFile.is_open())
        {
            while (getline(readFile, line))
            {
                lines.push_back(line);
            }
            readFile.close();
        }
        else
        {
            cout << "Error - Could not open " << filename << endl;
            exit(1);
        }
        return lines;
    }
    
    // Read parameters from config file
    void readConfigFile()
    {
        vector<string> lines;
        if (system_status == RECOVERY)
        {
            lines = readFile(outputName);
        }
        else
        {
            lines = readFile(configFile);
        }
        
        for (int i = 0;i < lines.size();i ++)
        {
            if (system_status == NORMAL && i == 0)
            {
                string address = lines[0];
                myAddress = address.erase(address.length() - 1, 1);
                i++;
            }
            
            vector<string> values = split(lines[i], ' ');
            bookingData.push_back(stoi(values[1]));
        }
    }
    
    VoteStatus checkRequest(Response r)
    {
        for (int i = 0;i < r.dates.size();i ++)
        {
            int ticketsAvail = bookingData[r.dates[i]];
            if (ticketsAvail < r.tickets)
            {
                return VOTE_NO;
            }
        }
        
        return VOTE_YES;
    }
    
    void outputBookingData()
    {
        string port = split(myAddress, ':')[1];
        outputName = "storage-hotel.txt";
        if (port == "6002")
        {
            outputName = "storage-concert.txt";
        }
        outputFile.open (outputName, ios::trunc);
        for (int i = 0;i < bookingData.size();i ++)
        {
            outputFile << (i + 1) << " " << bookingData[i] << endl;
        }
        outputFile.close();
    }
    
    void performAction(ActionType action)
    {
        if (action == COMMIT)
        {
            for (int i = 0;i < commitStorage.dates.size();i ++)
            {
                bookingData[commitStorage.dates[i] - 1] -= commitStorage.tickets;
            }
            outputBookingData();
        }
        else
        {
            commitStorage = Response();
        }
    }
    
    bool processRequest(Response res)
    {
        cout << "Recieved request id " << res.requestId << endl;
        
        VoteStatus vote = checkRequest(res);
        commitStorage = res;
        comm->sendVote(vote, res.requestId);
        
        return true;
    }
    
    bool processActionRequest(Response res)
    {
        cout << "Recieved commit id " << res.requestId << endl;
        
        performAction(res.action);
        comm->sendAck(res.requestId);
        
        cout << "2PC for id " << res.requestId << " complete." << endl;
        
        return true;
    }
    
    // Start the 2PC process
    bool twoPhaseCommit()
    {
        Response res = comm->waitForResponse();
        
        if (res.requestId == 0)
        {
            return false;
        }
        
        sleep(1);
        
        if (res.isRequest)
        {
            return processRequest(res);
        }
        else
        {
            return processActionRequest(res);
        }
    }
    
    // Function to start thread B
    static void * processThreadCaller(void * context)
    {
        return ((Participant *)context)->processBookingRequests(NULL);
    }
    
    // Threaded function to process requests
    void * processBookingRequests(void *)
    {
        while (system_status == NORMAL || system_status == RECOVERY)
        {
            if (twoPhaseCommit())
            {
                // Successful
            }
        }
        
        pthread_exit(NULL);
    }
    
    void initParticipant(string configFilename)
    {
        configFile = configFilename;
        
        cout << "Parsing config file..." << endl;
        readConfigFile();
        cout << "Participant initialization complete." << endl;
        
        logfile.open ("log.txt", ios::trunc);
        
        if (system_status == NORMAL)
        {
            comm = new CommunicationSubstrate(myAddress);
        }
    }
    
public:
    
    // ** Public Functions **
    
    // Constructor
    Participant(string configFilename)
    {
        initParticipant(configFilename);
    }
    
    void startServer()
    {
        cout << "Starting participant..." << endl;
        
        if (int s = pthread_create(&processThread, NULL, &Participant::processThreadCaller, this))
        {
            cout << "Error creating process thread. Code - " << s << endl;
            exit(1);
        }
        
        cout << "Participant started." << endl;
    }
    
    void failSystem()
    {
        system_status = FAILED;
        bookingData = vector<int>();
        commitStorage = Response();
        
        comm->failSystem();
        
        logfile << configFile << endl;
        
        logfile.close();
        outputFile.close();
        
        cout << "System failed and sleeping." << endl;
    }
    
    void recoverSystem()
    {
        system_status = RECOVERY;
        
        vector<string> lines = readFile("log.txt");
        configFile = lines[0];
        
        initParticipant(configFile);
        
        system_status = NORMAL;
        outputFile << "System Recovered" << endl;
        
        startServer();
    }
    
    void startFailureSimulation()
    {
        string command = "";
        while (command != "exit" && system_status != FINISHED)
        {
            cin >> command;
            if (command == "fail" && system_status == NORMAL)
            {
                cout << "System failing..." << endl;
                failSystem();
            }
            else if (command == "recover" && system_status == FAILED)
            {
                cout << "Starting recovery..." << endl;
                recoverSystem();
            }
        }
    }
};

// Main function
int main(int argc, const char * argv[])
{
    if (argc != 2)
    {
        cout << "Error - wrong command line arguments" << endl;
        return 1;
    }
    
    string configFile = argv[1];
    
    signal(SIGPIPE, SIG_IGN);
    
    Participant * coor = new Participant(configFile);
    coor->startServer();
    coor->startFailureSimulation();
    
    return 0;
}
