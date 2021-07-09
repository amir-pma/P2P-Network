import os
import json
import time
import socket
import signal
import pickle
import random
import networkx
import threading
from socket import *
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


class Config:
    ip = "127.0.0.1"
    nodesPorts = [10001, 10002, 10003, 10004, 10005, 10006]
    N = 3
    helloPeriod = 2
    timout = 8
    shutdownPeriod = 10
    shutdownTime = 20
    dropRate = 0.05
    runPeriod = 5 * 60
    maxRecvSize = 4096


class Manager:
    def __init__(self):
        self.nodesIDsList = self.generateNodesIDsList()
        self.nodes = []

    def start(self):
        global config
        self.generateNodes()
        self.shutDowner = threading.Timer(config.shutdownPeriod, self.shutdownOneNode)
        self.shutDowner.start()
        time.sleep(config.runPeriod)
        self.end()

    def end(self):
        self.shutDowner.cancel()
        for thread in self.nodes:
            thread.cancel()
        self.reportStatistics()

    def shutdownOneNode(self):
        global config
        chosenNode = random.choice(self.nodes)
        while(not chosenNode.isActive):
            chosenNode = random.choice(self.nodes)
        chosenNode.deactiveiate()
        self.shutDowner = threading.Timer(config.shutdownPeriod, self.shutdownOneNode)
        self.shutDowner.start()

    def generateNodesIDsList(self):
        global config
        return [(config.ip, port) for port in config.nodesPorts]

    def generateNodes(self):
        for nodeId in self.nodesIDsList:
            nodeThread = Node(nodeId, self.nodesIDsList)
            nodeThread.daemon = True
            self.nodes.append(nodeThread)
            nodeThread.start()

    def reportStatistics(self):
        if(os.name == "nt"):
            if(os.path.exists("logs")):
                os.system("rmdir logs /S /Q")
        elif(os.name == "posix"):
            os.system("rm -rf logs")
        os.mkdir("logs")
        for node in self.nodes:
            self.writeNodeLog(node)

    def writeNodeLog(self, node):
        global startTime
        dirName = "logs/" + str(node.nodeId[1])
        os.mkdir(dirName)
        reportFile = {
            "Connected Neighbors History": [],
            "Current Neighbours": [],
            "Neighbors Availability": {},
            "Topology": {}
        }
        reportFile["Connected Neighbors History"] = self.getConnectedNeighborsHistory(node)
        reportFile["Current Neighbours"] = self.getCurrentNeighbours(node)
        reportFile["Neighbors Availability"] = self.getAvailabilityToOtherNodes(node)
        reportFile["Topology"] = self.getTopology(node, dirName)
        file = open(dirName + "/Statistics.json", "w")
        file.write(json.dumps(reportFile, indent=4))
        file.close()
    
    def getConnectedNeighborsHistory(self, node):
        result = []
        for nodeId in self.nodesIDsList:
            if(node.reportInfo[nodeId].hasConnected):
                result.append({
                    "IP": nodeId[0],
                    "Port": nodeId[1],
                    "Recieved Packets": node.reportInfo[nodeId].recieved,
                    "Sent Packets": node.reportInfo[nodeId].sent
                })
        return result

    def getCurrentNeighbours(self, node):
        result = []
        for neighbor in node.biNeighbors:
            result.append({
                "IP": neighbor.neighborId[0],
                "Port": neighbor.neighborId[1]
            })
        return result
    
    def getAvailabilityToOtherNodes(self, node):
        global config
        result = {}
        for nodeId in self.nodesIDsList:
            if(nodeId != node.nodeId):
                result[nodeId[1]] = {
                    "Seconds": int(node.reportInfo[nodeId].totalConnectTime),
                    "Percent": int(node.reportInfo[nodeId].totalConnectTime / config.runPeriod * 100)
                }
        return result

    def getTopology(self, node, dirName):
        result = {"Vertexes": [], "Edges": []}
        for nodeId in self.nodesIDsList:
            result["Vertexes"].append(nodeId[1])
        for neighbor in node.biNeighbors:
            result["Edges"].append(self.makeTopologyEdge(node.nodeId, neighbor.neighborId, "BiDirectional"))
        for neighbor in node.uniNeighbors:
            result["Edges"].append(self.makeTopologyEdge(node.nodeId, neighbor.neighborId, "UniDirectional"))
        seen = []
        for nodeId in node.reportInfo:
            if(nodeId != node.nodeId):
                for neighborId in node.reportInfo[nodeId].biNeighborsIds:
                    if((neighborId != node.nodeId) and ((neighborId, nodeId) not in seen)):
                        seen.append((nodeId, neighborId))
                        result["Edges"].append(self.makeTopologyEdge(nodeId, neighborId, "BiDirectional"))
        self.drawTopologies(result, node.nodeId, dirName)
        return result
    
    def makeTopologyEdge(self, fromId, toId, connectionType):
        return {
            "From": {
                "IP": fromId[0],
                "Port": fromId[1]
            },
            "To": {
                "IP": toId[0],
                "Port": toId[1]
            },
            "Connection Type": connectionType
        }
    
    def drawTopologies(self, result, nodeId, dirName):
        graph = networkx.DiGraph()
        graph.add_nodes_from(result["Vertexes"])
        for edge in result["Edges"]:
            if(edge["Connection Type"] == "UniDirectional"):
                graph.add_edge(edge["From"]["Port"], edge["To"]["Port"])
            elif(edge["Connection Type"] == "BiDirectional"):
                graph.add_edge(edge["From"]["Port"], edge["To"]["Port"])
                graph.add_edge(edge["To"]["Port"], edge["From"]["Port"])
        pos = networkx.circular_layout(graph)
        plt.figure(figsize=(10, 10))
        plt.margins(0.1)
        colors = []
        for port in result["Vertexes"]:
            if(port == nodeId[1]):
                colors.append("yellow")
            else:
                colors.append("cyan")
        networkx.draw(graph, pos, with_labels=True, node_color=colors, node_size=10000)
        plt.savefig(dirName + "/" + "Topology.png")


class NeighborInfo:
    def __init__(self, neighborId_, node_):
        self.neighborId = neighborId_
        self.node = node_
        self.checker = threading.Timer(config.timout, self.node.deleteNeighbor, args=(self,))
        self.checker.start()

    def cancel(self):
        self.checker.cancel()

    def restart(self):
        global config
        self.checker.cancel()
        self.checker = threading.Timer(config.timout, self.node.deleteNeighbor, args=(self,))
        self.checker.start()


class ReportInfo:
    def __init__(self):
        self.hasConnected = False
        self.recieved = 0
        self.sent = 0
        self.lastRecieveTime = -1
        self.lastSendTime = -1
        self.connectionStartTime = -1
        self.totalConnectTime = 0
        self.biNeighborsIds = []


class Node(threading.Thread):
    def __init__(self, nodeId_, nodesIDsList_):
        threading.Thread.__init__(self)
        self.nodeId = nodeId_
        self.nodesIDsList = nodesIDsList_
        self.biNeighbors = []
        self.uniNeighbors = []
        self.searchingNeighbors = []
        self.isActive = True
        self.reportInfo = {}
        self.activator = None
        for nodeId in self.nodesIDsList:
            self.reportInfo[nodeId] = ReportInfo()
    
    def run(self):
        self.initializeSocket()
        self.generateFirstSearchingNeighbors()
        self.startSendingTimer()
        self.recieve()

    def cancel(self):
        if((self.activator != None) and (not self.isActive)):
            self.activator.cancel()
        self.isActive = False
        self.sendingTimer.cancel()
        self.deactiveNeighbors(False)
        self.socket.close()

    def deactiveiate(self):
        global config
        self.isActive = False
        self.deactiveNeighbors(True)
        self.activator = threading.Timer(config.shutdownTime, self.activeiate)
        self.activator.start()

    def deactiveNeighbors(self, shouldClear):
        for n in (self.biNeighbors + self.uniNeighbors + self.searchingNeighbors):
            n.cancel()
        for neighbor in self.biNeighbors:
            self.reportInfo[neighbor.neighborId].totalConnectTime += time.time() - self.reportInfo[neighbor.neighborId].connectionStartTime
        if(shouldClear):
            self.uniNeighbors = []
            self.biNeighbors = []
            self.searchingNeighbors = []
            
    def activeiate(self):
        self.generateFirstSearchingNeighbors()
        self.isActive = True

    def initializeSocket(self):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind(self.nodeId)

    def startSendingTimer(self):
        self.sendingTimer = SendingTimerThread(self)
        self.sendingTimer.daemon = True
        self.sendingTimer.start()

    def generateFirstSearchingNeighbors(self):
        for neighborId in self.nodesIDsList:
            if(neighborId != self.nodeId):
                self.searchingNeighbors.append(NeighborInfo(neighborId, self))

    def deleteNeighbor(self, neighbor):
        neighbor.cancel()
        if(neighbor in self.biNeighbors):
            self.reportInfo[neighbor.neighborId].totalConnectTime += time.time() - self.reportInfo[neighbor.neighborId].connectionStartTime
            self.biNeighbors.remove(neighbor)
        elif(neighbor in self.uniNeighbors):
            self.uniNeighbors.remove(neighbor)
        elif(neighbor in self.searchingNeighbors):
            self.searchingNeighbors.remove(neighbor)
        self.searchCheck()

    def searchCheck(self):
        global config
        if(len(self.biNeighbors) < config.N):
            searchList = list(self.nodesIDsList)
            random.shuffle(searchList)
            finalChosen = None
            for chosen in searchList:
                if((chosen != self.nodeId) and (chosen not in [n.neighborId for n in self.biNeighbors]) and (chosen not in [n.neighborId for n in self.uniNeighbors]) and (chosen not in [n.neighborId for n in self.searchingNeighbors])):
                    finalChosen = chosen
                    break
            if(finalChosen != None):
                self.searchingNeighbors.append(NeighborInfo(finalChosen, self))

    def recieve(self):
        global config
        while(True):
            try:
                data, addr = self.socket.recvfrom(config.maxRecvSize)
                helloPacket = pickle.loads(data)
                handlerThread = threading.Thread(target=self.handleRecv, args=(helloPacket,))
                handlerThread.daemon = True
                handlerThread.start()
            except:
                break
    
    def handleRecv(self, helloPacket):
        global config, startTime
        if((random.random() < config.dropRate) or (not self.isActive)):
            return
        isInBiNeighbors, isInUniNeighbors, isInSearchingNeighbors = self.checkIsInNeighbors(helloPacket.senderId)
        isInHeard = (self.nodeId in helloPacket.uniNeighbors) or (self.nodeId in helloPacket.biNeighbors)
        if((not isInBiNeighbors) and (len(self.biNeighbors) == config.N)):
            return
        neighbor = None
        if(isInBiNeighbors or isInUniNeighbors or isInSearchingNeighbors):
            neighbor = self.findNeighbor(helloPacket.senderId)
            neighbor.restart()
        self.reportInfo[helloPacket.senderId].lastRecieveTime = time.time() - startTime
        if(isInBiNeighbors):
            self.reportInfo[neighbor.neighborId].recieved += 1
            self.updateTopology(helloPacket)
            if(not isInHeard):
                self.move(neighbor, self.biNeighbors, self.uniNeighbors)
        elif(isInUniNeighbors):
            if(isInHeard):
                self.reportMoveToBi(neighbor.neighborId)
                self.move(neighbor, self.uniNeighbors, self.biNeighbors)
        elif(isInSearchingNeighbors):
            if(isInHeard):
                self.reportMoveToBi(neighbor.neighborId)
                self.move(neighbor, self.searchingNeighbors, self.biNeighbors)
            else:
                self.move(neighbor, self.searchingNeighbors, self.uniNeighbors)
        else:
            if(isInHeard):
                self.reportMoveToBi(helloPacket.senderId)
                self.biNeighbors.append(NeighborInfo(helloPacket.senderId, self))
            else:
                self.uniNeighbors.append(NeighborInfo(helloPacket.senderId, self))
    
    def reportMoveToBi(self, neighborId):
        self.reportInfo[neighborId].hasConnected = True
        self.reportInfo[neighborId].connectionStartTime = time.time()

    def checkIsInNeighbors(self, senderId):
        isInBiNeighbors = senderId in [n.neighborId for n in self.biNeighbors]
        isInUniNeighbors = senderId in [n.neighborId for n in self.uniNeighbors]
        isInSearchingNeighbors = senderId in [n.neighborId for n in self.searchingNeighbors]
        return isInBiNeighbors, isInUniNeighbors, isInSearchingNeighbors
    
    def findNeighbor(self, senderId):
        for n in self.biNeighbors:
            if(n.neighborId == senderId):
                return n
        for n in self.uniNeighbors:
            if(n.neighborId == senderId):
                return n
        for n in self.searchingNeighbors:
            if(n.neighborId == senderId):
                return n

    def move(self, neighbor, fromList, toList):
        if(neighbor in fromList):
            fromList.remove(neighbor)
        toList.append(neighbor)

    def updateTopology(self, helloPacket):
        self.reportInfo[helloPacket.senderId].biNeighborsIds = helloPacket.biNeighbors
        for neighborId in self.reportInfo:
            if((neighborId != self.nodeId) and (helloPacket.senderId in self.reportInfo[neighborId].biNeighborsIds) and (neighborId not in helloPacket.biNeighbors)):
                self.reportInfo[neighborId].biNeighborsIds.remove(helloPacket.senderId)


class SendingTimerThread(threading.Thread):
    def __init__(self, node_):
        threading.Thread.__init__(self)
        self.node = node_
        self.event = threading.Event()

    def cancel(self):
        self.event.set()

    def run(self):
        global config, startTime
        while(not self.event.wait(config.helloPeriod)):
            if(self.node.isActive):
                helloRecievers = []
                if(len(self.node.biNeighbors) == config.N):
                    helloRecievers = self.node.biNeighbors
                else:
                    helloRecievers = self.node.biNeighbors + self.node.uniNeighbors + self.node.searchingNeighbors
                for neighbor in helloRecievers:
                    self.sendHelloPacket(neighbor)
                    self.node.reportInfo[neighbor.neighborId].lastSendTime = time.time() - startTime

    def sendHelloPacket(self, neighbor):
        if(neighbor in self.node.biNeighbors):
            self.node.reportInfo[neighbor.neighborId].sent += 1
        packet = HelloPacket(self.node.nodeId, self.node.reportInfo[neighbor.neighborId].lastRecieveTime, self.node.reportInfo[neighbor.neighborId].lastSendTime, self.node.uniNeighbors, self.node.biNeighbors)
        packetBytes = pickle.dumps(packet)
        if(self.node.isActive):
            self.node.socket.sendto(packetBytes, neighbor.neighborId)


class HelloPacket:
    def __init__(self, senderID_, lastRecieveTime_, lastSendTime_, uniNeighbors, biNeighbors):
        self.senderId = senderID_
        self.senderIP = senderID_[0]
        self.senderPort = senderID_[1]
        self.packetType = "HelloType"
        self.uniNeighbors = [n.neighborId for n in uniNeighbors]
        self.biNeighbors = [n.neighborId for n in biNeighbors]
        self.lastRecieveTime = lastRecieveTime_
        self.lastSendTime = lastSendTime_


startTime = time.time()
config = Config()
manager = Manager()
manager.start()