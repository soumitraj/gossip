/* Gossip and Pusch sum simulation in Scala*/

package gossip;

import scala.math._
import scala.util.Random
import akka.actor.Actor
import akka.actor.Props
import akka.actor._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props

case class Start
case class Gossip
case class PushSumGossip(sum:Double, weight:Double)
case class InitWorkerNode(id:Int, workerNodeList:List[ActorRef], neighbor:List[Int], listener:ActorRef, rumorRecieveLimit:Int)
case class GossipRecieved(nodeId:Int, gossipCount:Int)
case class PushSumConverged(nodeId:Int)
case class InitListener(system:ActorSystem, nodeCount:Int, rumorRecieveLimit:Int, algorithm:String)

object HelloMain  {
 
def main(args : Array[String]){
	  if(args.length == 0 || args.length != 3){
      println("Invalid inputs")
    }else if(args.length == 3){
      
      var numNodes:Int = args(0).toInt
      var topology:String = args(1)
      var algorithm:String = args(2)
      
	val system = ActorSystem("GossipSystem")
	var listener:ActorRef = system.actorOf(Props[Master])
	val masterActor = system.actorOf(Props(new GossipMaster(
				numNodes, topology, algorithm,system, listener)),
				name = "MasterActor")
	 masterActor ! Start			
     }
      
}
}

class GossipMaster(var numNodes:Int, topology:String ,algorithm:String ,system:ActorSystem, listener: ActorRef)
	extends Actor {
    val failures = 300
      
      val rumorRecieveLimit:Int = 10;
       var workerNodeList:List[ActorRef] = Nil
    
    def createNodes(system:ActorSystem,numNodes:Int) = {
	 var i:Int = 0
      while(i<numNodes){
        workerNodeList ::= system.actorOf(Props[WorkerNode])
        i += 1
      }
	}        
    
    def startGossip(algorithm:String,numNodes:Int) = {
    	 if(algorithm.equalsIgnoreCase("gossip")){
        workerNodeList(0) ! Gossip
      }else{
     
          var randomWorkerNode = Random.nextInt(workerNodeList.length)
          workerNodeList(randomWorkerNode) ! PushSumGossip(0,1)
      
      }
    }
 
  
  def buildNetwork(topology:String,listener:ActorRef,numNodesRoot:Double) = {
  	
  	if("full".equalsIgnoreCase(topology)){
          println("Inside Full ...")
          var i:Int = 0
          while(i<workerNodeList.length){
            var neighbors:List[Int] = Nil
            
            var j:Int = 0
            while(j<workerNodeList.length){
              if(j!=i){
                neighbors ::= j
              }
              j += 1
            }
            workerNodeList(i) ! InitWorkerNode(i, workerNodeList, neighbors, listener, rumorRecieveLimit)
            i += 1
          }
         println("Completed Full ...")
        }
      
        if("2D".equalsIgnoreCase(topology)){
          var i:Int = 0
          while(i<workerNodeList.length){
            var neighbors:List[Int] = Nil
            
            if(i.toDouble<(workerNodeList.length.toDouble - math.sqrt(workerNodeList.length.toDouble)))
            	neighbors ::= (i+numNodesRoot.toInt)
            if(i.toDouble>=math.sqrt(workerNodeList.length.toDouble))
             neighbors ::= (i-numNodesRoot.toInt)
            if(i.toDouble % math.sqrt(workerNodeList.length)!=0) 
             neighbors ::= (i-1)
            if((i.toDouble+1) % math.sqrt(workerNodeList.length)!=0) 
             neighbors ::= (i+1)
            
            workerNodeList(i) ! InitWorkerNode(i, workerNodeList, neighbors, listener, rumorRecieveLimit)
            
            i += 1
          }
        }
        if("line".equalsIgnoreCase(topology)){
          var i:Int = 0;
          while(i<workerNodeList.length){
            var neighbors:List[Int] = Nil
            
            if(i>0) neighbors ::= (i-1)
            if(i<workerNodeList.length-1) neighbors ::= (i+1)
            
            workerNodeList(i) ! InitWorkerNode(i, workerNodeList, neighbors, listener, rumorRecieveLimit)
            
            i += 1
          }
        }
        
        if("imp2D".equalsIgnoreCase(topology)){
          var i:Int = 0
          while(i<workerNodeList.length){
            var neighbors:List[Int] = Nil
            var tempList:List[Int] = Nil
            
             if(!(i.toDouble>=(workerNodeList.length.toDouble - math.sqrt(workerNodeList.length.toDouble))))
            	 { neighbors ::= (i+numNodesRoot.toInt); tempList ::= (i+numNodesRoot.toInt); }
            if(!(i.toDouble<math.sqrt(workerNodeList.length.toDouble))) 
            	{ neighbors ::= (i-numNodesRoot.toInt); tempList ::= (i-numNodesRoot.toInt); }
            if(i.toDouble % math.sqrt(workerNodeList.length)!=0)  { neighbors ::= (i-1); tempList ::= (i-1); }
            if((i.toDouble+1) % math.sqrt(workerNodeList.length)!=0)  { neighbors ::= (i+1); tempList ::= (i+1); }
            
            var randomInt:Int = -1;
            do{
              randomInt = Random.nextInt(workerNodeList.length)
              for(x<-tempList){
                if(randomInt == x) randomInt = -1
              }   
            }while(randomInt == -1)
            
            neighbors ::= (randomInt)
            workerNodeList(i) ! InitWorkerNode(i, workerNodeList, neighbors, listener, rumorRecieveLimit)
            
            i += 1
          }
        }
  
  }
  
  
  def receive = {
		case Start => {
			if(topology=="2D" || topology=="imp2D"){ 
   				   while(math.sqrt(numNodes.toDouble)%1!=0){
     				   numNodes+=1
     			 }
			}
      		val numNodesRoot = math.sqrt(numNodes.toDouble);
   	      	createNodes(system,numNodes)      
  		    	buildNetwork(topology,listener,numNodesRoot)  
      		listener ! InitListener(system, numNodes, rumorRecieveLimit, algorithm)
      		
      		for(i <- 1 to failures)
 			{
   					 val tar = workerNodeList(Random.nextInt(workerNodeList.length))
    					 println("stopping")
   				 	 println(tar.path.name)
  					 context.stop(tar)
  			}
      		
      		startGossip(algorithm,numNodes)
		}
	}
  
}

case class WorkerNode extends Actor{
	
	  import context._
	  import scala.concurrent.duration._
//
	
    var rumorTerminationGossip = 0
    var rumorTerminationPush = math.pow(10, -10)
    var nodeId:Int = 0
    var workerNodeList:List[ActorRef] = Nil
    var schedulor:akka.actor.Cancellable = _
    var listener:ActorRef = null
    var neighbors:List[Int] = Nil
    var rumorCounter:Int = 0
    var stabilityCounter:Int = 0;
    var s:Double = 1
    var w:Double = 1
    var tickFlag= 0
    
    def receive = {
    
    		case "tick" => {
    			self!PushSumGossip(s, w)
    		}
    		
    		case "tickGossip" => {
    			self ! Gossip
    		}
    		
        case InitWorkerNode(id:Int, allNodes:List[ActorRef], neighborList:List[Int], statActor:ActorRef, rumorRecieveLimit:Int) => {
          neighbors = neighbors ::: neighborList
          nodeId = id
          listener = statActor
          rumorTerminationGossip = rumorRecieveLimit
          //println("initialize called for node id :"+id)
          s = id+1
          w=1
          workerNodeList = allNodes
          
        }
        
        
        case Gossip => {
        
       	if(tickFlag != 0 ){
          	schedulor.cancel()
          }else{
          	tickFlag = 1
          }
           schedulor = context.system.scheduler.schedule(500 millis, 1000 millis, self, "tickGossip")
        
        
          if(rumorCounter<rumorTerminationGossip) {
            //println("Received ping to id: "+nodeId+" Counter -- "+rumorCounter)
        	rumorCounter += 1;
        	listener ! GossipRecieved(nodeId, rumorCounter)
        	
            var randomWorkerNode = 0
            
              randomWorkerNode = Random.nextInt(neighbors.length)
             //println("From Node: "+nodeId+" Sending message to "+randomWorkerNode)
              workerNodeList(neighbors(randomWorkerNode)) ! Gossip
          }
        }
        
        case PushSumGossip(sum:Double, weight:Double) => {
          //println("WorkerNodeID: "+nodeId+"  Receives S/W: "+(sum/weight)+" -- Self s/w: "+(s/w));
         /* if(tickFlag != 0 ){
         		schedulor.cancel()
          }else{
          	tickFlag = 1
          }
           schedulor = context.system.scheduler.schedule(500 millis, 1000 millis, self, "tick")
          */
          rumorCounter += 1;
          var oldRatio:Double = s/w;
          s+=sum;
	      w+=weight;
	      s = s/2;
	      w = w/2;
	      var newRatio:Double = s/w;
	      
	      //println("Inside PushSum ---- oldRatio: "+oldRatio+"   --- newRatio: "+newRatio);
	      
          if(rumorCounter==1 || Math.abs((oldRatio-newRatio))>rumorTerminationPush) {
        	  stabilityCounter=0;
        	  
	            //println("From Actor: "+nodeId+"\tSending Message ... Sum: "+s+" Weight: "+w);
	            var randomWorkerNode = Random.nextInt(neighbors.length);
	            workerNodeList(neighbors(randomWorkerNode)) ! PushSumGossip(s,w)
	          
          }else{
            stabilityCounter+=1;
            if(stabilityCounter>3) {
              println("Final Condition --- For Actor  "+nodeId+" \tRumor Count  "+rumorCounter+" \ts/w: "+(s/w));
              
              listener ! PushSumConverged(nodeId); 
	          self ! PoisonPill
	        }else{
	            //println("From Actor: "+nodeId+" Sending Message ... Sum: "+s+" Weight: "+w);
	            var randomWorkerNode = Random.nextInt(neighbors.length);
	            workerNodeList(neighbors(randomWorkerNode)) ! PushSumGossip(s,w)
	        }
          }
        }
    }
    override def postStop(){
    	//println("Post Terimation --- For Actor  "+nodeId+" \tRumor Count  "+rumorCounter+" \ts/w: "+(s/w));
    }
}



class Master extends Actor{
	var b:Long = 0;
	var numNodes:Int = 0;
	var rumorRecieveLimit:Int = 0;
    var keeperSys:ActorSystem = null;
    var shouldWork:Boolean = false;
    var stableNodeCount:Int = 0;
    
    println("Status Keeper Started ...")
    b = System.currentTimeMillis
    println("Start Time: "+b)
    var statusList:List[GossipRecieved] = Nil
    var psStatusList:List[PushSumConverged] = Nil
    
      def receive = {
        case InitListener(sys:ActorSystem, nodeCount:Int, rumorBreakLimit:Int, algorithm:String) => {
          b = System.currentTimeMillis
          keeperSys = sys;
          numNodes = nodeCount;
          rumorRecieveLimit = rumorBreakLimit;
          if(algorithm.equalsIgnoreCase("gossip")) shouldWork = true;
        }
        
        case GossipRecieved(id:Int, count:Int) => {
          var tempStatusList:List[GossipRecieved] = Nil
          var i:Int = 0
          while(i<statusList.length){
            if(statusList(i).nodeId!=id) tempStatusList ::= statusList(i) 
            i += 1
          }
          statusList = tempStatusList
          statusList = statusList ::: List(new GossipRecieved(id, count))
          
          //println("Status received from WorkerNode"+id+"  count: "+count+ "  StatusListCount : "+statusList.length)
          val failures = 300
          if((statusList.length.toDouble/(numNodes-failures).toDouble)==1){
            keeperSys.shutdown;
          }
        }
        
        case PushSumConverged(nodeId:Int) => {
          println("Stability acheived by :"+nodeId+"  stableNodeCount: "+stableNodeCount+"  numNodes: "+numNodes);
          keeperSys.shutdown;
        }
      }
  
	  override def postStop(){
	    println("Post Stop Test Text ...")
	    
	        if(shouldWork){
	      for(r <-0 until rumorRecieveLimit+1){
	        var count:Int = 0;
	        for(status<-statusList){
	          if(status.gossipCount==r) count += 1;
	        }
	        if(r==0) count = (numNodes - statusList.length)
	        println("Number of Actor who received Gossip "+r+" times : "+count);
	      }
	      println("Total Coverage: "+statusList.length+" / "+numNodes+" => "+100*(statusList.length.toDouble/numNodes.toDouble)+"%")
	    }
	    println(b-System.currentTimeMillis)
	    
	  }
	  
}

