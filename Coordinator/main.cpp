//
//  main.cpp
//  Coordinator
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
};

struct Response
{
    int requestId = 0;
    bool ack;
    VoteStatus status;
    int socket;
    
    static Response createFromPacket(Packet p)
    {
        Response res;
        
        res.ack = (p.length == sizeof(int) * 2);
        res.requestId = p.data[1];
        if (!res.ack)
        {
            res.status = VoteStatus(p.data[2]);
        }
        res.socket = p.socket;
        
        return res;
    }
};

struct BookingRequest
{
    int id;
    int tickets;
    vector<int> dates;
    
    void print()
    {
        cout << id << " - " << tickets << " - ";
        for (int i = 0;i < dates.size();i ++)
        {
            cout << dates[i] << " ";
        }
        cout << endl;
    }
    
    Packet getPacket(int socket)
    {
        Packet p;
        
        int l = (int) dates.size() + 4;
        
        time_t currentTime;
        time(&currentTime);
        p.timestamp = (int)currentTime;
        
        int * intData = new int[l];
        intData[0] = p.timestamp;
        intData[1] = id;
        intData[2] = tickets;
        intData[3] = (int) dates.size();
        for (int i = 0;i < dates.size();i ++)
        {
            intData[4 + i] = dates[i];
        }
        
        p.data = intData;
        p.length = sizeof(int) * l;
        p.socket = socket;
        
        return p;
    }
    
    Packet createActionPacket(int socket, ActionType action)
    {
        Packet p;
        
        p.socket = socket;
        
        int * data = new int[3];
        
        time_t currentTime;
        time(&currentTime);
        
        data[0] = (int)currentTime;
        data[1] = id;
        data[2] = action;
        
        p.data = data;
        p.length = sizeof(int) * 3;
        p.timestamp = data[0];
        
        return p;
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
    
    sockaddr_in hotel;
    sockaddr_in concert;
    
    int hotelSocket;
    int concertSocket;
    
    queue<Response> responseBuffer;
    
    // ** Private Functions **
    
    void populateIPAddress(sockaddr_in * address, string addressInfo)
    {
        address->sin_family = AF_INET;
        vector<string> addressParts = split(addressInfo, ':');
        address->sin_port = htons(stoi(addressParts[1]));
        inet_pton(AF_INET, addressParts[0].c_str(), &address->sin_addr);
    }
    
    void connectToParticipants()
    {
        if (connect(hotelSocket, (sockaddr *)&hotel, sizeof(hotel)) != 0)
        {
            cout << "Error - Couldn't connect to hotel participant" << endl;
            exit(1);
        }
        
        if (connect(concertSocket, (sockaddr *)&concert, sizeof(concert)) != 0)
        {
            cout << "Error - Couldn't connect to concert participant" << endl;
            exit(1);
        }
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
            recieveDataFromSocket(hotelSocket, buffer);
            recieveDataFromSocket(concertSocket, buffer);
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
    
    CommunicationSubstrate(string hotelIP, string concertIP)
    {
        populateIPAddress(&hotel, hotelIP);
        populateIPAddress(&concert, concertIP);
        
        hotelSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        concertSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        
        connectToParticipants();
        
        startSubstrate();
    }
    
    bool sendRequest(BookingRequest req)
    {
        cout << "Sending request " << req.id << endl;
        
        Packet hotelPacket = req.getPacket(hotelSocket);
        Packet concertPacket = req.getPacket(concertSocket);
        
        outputBuffer.push(hotelPacket);
        outputBuffer.push(concertPacket);
        
        return true;
    }
    
    vector<Response> waitForResponse()
    {
        cout << "Waiting for response..." << endl;
        bool hotelRes = false;
        bool concertRes = false;
        vector<Response> responses;
        
        time_t startTime;
        time_t currentTime;
        time(&startTime);
        
        while ((!hotelRes || !concertRes) && system_status == NORMAL)
        {
            time(&currentTime);
            if (currentTime - startTime > 10)
            {
                cout << "Timeout..." << endl;
                break;
            }
            
            if (!responseBuffer.empty())
            {
                Response r = responseBuffer.front();
                if (r.socket == hotelSocket)
                {
                    cout << "Recieved hotel " << (r.ack ? "acknowledgement " : (r.status ? "vote yes " : "vote no ")) << r.requestId << endl;
                    hotelRes = true;
                    responses.push_back(r);
                }
                else if (r.socket == concertSocket)
                {
                    cout << "Recieved concert " << (r.ack ? "acknowledgement " : (r.status ? "vote yes " : "vote no ")) << r.requestId << endl;
                    concertRes = true;
                    responses.push_back(r);
                }
                responseBuffer.pop();
            }
        }
        
        return responses;
    }
    
    bool sendAction(BookingRequest req, ActionType action)
    {
        cout << "Sending " << (action == COMMIT ? "Commit " : "Rollback ") << req.id << endl;
        
        Packet hotelAction = req.createActionPacket(hotelSocket, action);
        Packet concertAction = req.createActionPacket(concertSocket, action);
        
        outputBuffer.push(hotelAction);
        outputBuffer.push(concertAction);
        
        return true;
    }
    
    void stopSubstrate()
    {
        if (system_status == FINISHED)
        {
            Packet finishPacket;
            finishPacket.data = new int[1];
            finishPacket.data[0] = 0;
            finishPacket.length = sizeof(int);
            finishPacket.socket = hotelSocket;
            finishPacket.sendPacket();
            finishPacket.socket = concertSocket;
            finishPacket.sendPacket();
        }
        
        close(hotelSocket);
        close(concertSocket);
    }
    
    void failSystem()
    {
        outputBuffer = queue<Packet>();
        inputBuffer = queue<Packet>();
        responseBuffer = queue<Response>();
        
        cout << "Communication Substrate failed." << endl;
    }
};

class Coordinator
{
private:
    
    // ** Class Parameters **
    
    string configFile;
    string bookingFile;
    
    string hotelIP;
    string concertIP;
    
    queue<BookingRequest> requests;
    
    pthread_t processThread;
    
    CommunicationSubstrate * comm;
    
    BookingRequest currentRequest;
    
    ofstream outputFile;
    ofstream logfile;
    
    int currentRecord = 0;
    
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
        vector<string> lines = readFile(configFile);
        
        hotelIP = lines[0];
        concertIP = lines[1];
        bookingFile = lines[2];
    }
    
    // Parse booking file line into a BookingRequest
    BookingRequest parseBookingLine(string line)
    {
        vector<string> data = split(line, ' ');
        
        BookingRequest req;
        req.id = stoi(data[0]);
        req.tickets = stoi(data[1]);
        
        for (int i = 2;i < data.size();i ++)
        {
            string dateString = data[i];
            
            if (i == 2)
            {
                dateString = dateString.substr(1, 1);
            }
            else if (i == data.size() - 1)
            {
                dateString = dateString.substr(0, 1);
            }
            
            int date = stoi(dateString);
            req.dates.push_back(date);
        }
        
        return req;
    }
    
    // Read parameters from the booking file
    void readBookingFile()
    {
        vector<string> lines = readFile(bookingFile);
        
        for (int i = 0;i < lines.size();i ++)
        {
            string line = lines[i];
            BookingRequest req = parseBookingLine(line);
            requests.push(req);
        }
    }
    
    // Start the 2PC process
    bool twoPhaseCommit()
    {
        bool status = comm->sendRequest(currentRequest);
        
        if (!status) {return false;}
        
        vector<Response> res = comm->waitForResponse();
        
        if (res.size() != 2)
        {
            cout << "Response timeout for id " << currentRequest.id << endl;
            return false;
        }
        
        Response r1 = res[0];
        Response r2 = res[1];
        
        if (r1.requestId != currentRequest.id || r2.requestId != currentRequest.id)
        {
            cout << "Error - Expected id " << currentRequest.id << ", recieved " << r1.requestId << " and " << r2.requestId << endl;
            return false;
        }
        
        if (r1.status == VOTE_YES && r2.status == VOTE_YES)
        {
            status = comm->sendAction(currentRequest, COMMIT);
            outputFile << currentRequest.id << " Success" << endl;
        }
        else
        {
            status = comm->sendAction(currentRequest, ROLLBACK);
            outputFile << currentRequest.id << " Fail" << endl;
        }
        
        res = comm->waitForResponse();
        
        if (res.size() != 2)
        {
            cout << "Response timeout for id " << currentRequest.id << endl;
            return false;
        }
        
        cout << "2PC for " << currentRequest.id << " complete." << endl;
        
        return true;
    }
    
    void finishSystem()
    {
        cout << "All requests processed" << endl;
        outputFile.close();
        logfile.close();
        system_status = FINISHED;
        comm->stopSubstrate();
        exit(0);
    }
    
    // Function to start thread B
    static void * processThreadCaller(void * context)
    {
        return ((Coordinator *)context)->processBookingRequests(NULL);
    }
    
    // Threaded function to process requests
    void * processBookingRequests(void *)
    {
        if (system_status == RECOVERY)
        {
            for (int i = 0;i < currentRecord;i ++)
            {
                requests.pop();
            }
            
            system_status = NORMAL;
            cout << "System fully recovered." << endl;
        }
        
        while (!requests.empty() && system_status == NORMAL)
        {
            currentRequest = requests.front();
            if (twoPhaseCommit())
            {
                requests.pop();
                currentRecord ++;
                sleep(2);
            }
        }
        
        if (system_status == NORMAL)
        {
            finishSystem();
        }
        pthread_exit(NULL);
    }
    
    void initCoordinator(string configFilename)
    {
        configFile = configFilename;
        bookingFile = "";
        
        hotelIP = "127.0.0.1";
        concertIP = "127.0.0.1";
        
        cout << "Parsing config and booking files..." << endl;
        readConfigFile();
        readBookingFile();
        cout << "Coordinator initialization complete." << endl;
        
        logfile.open ("log.txt", ios::trunc);
        if (system_status == RECOVERY)
        {
            outputFile.open ("output.txt", ios::app);
        }
        else
        {
            outputFile.open ("output.txt", ios::trunc);
            comm = new CommunicationSubstrate(hotelIP, concertIP);
        }
    }
    
public:
    
    // ** Public Functions **
    
    // Constructor
    Coordinator(string configFilename)
    {
        initCoordinator(configFilename);
    }
    
    void startServer()
    {
        cout << "Starting coordinator..." << endl;
        
        if (int s = pthread_create(&processThread, NULL, &Coordinator::processThreadCaller, this))
        {
            cout << "Error creating process thread. Code - " << s << endl;
            exit(1);
        }
        
        cout << "Coordinator started." << endl;
    }
    
    void failSystem()
    {
        system_status = FAILED;
        requests = queue<BookingRequest>();
        
        comm->failSystem();
        
        logfile << configFile << endl;
        logfile << currentRecord << endl;
        
        outputFile << "System Failed." << endl;
        
        logfile.close();
        outputFile.close();
        
        cout << "System failed and sleeping." << endl;
    }
    
    void recoverSystem()
    {
        system_status = RECOVERY;
        
        vector<string> lines = readFile("log.txt");
        configFile = lines[0];
        currentRecord = stoi(lines[1]);
        cout << "Starting on record " << currentRecord << endl;
        
        initCoordinator(configFile);
        
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
    
    Coordinator * coor = new Coordinator(configFile);
    coor->startServer();
    coor->startFailureSimulation();
    
    return 0;
}
