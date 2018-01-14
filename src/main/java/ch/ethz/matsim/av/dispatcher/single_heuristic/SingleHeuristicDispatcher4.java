package ch.ethz.matsim.av.dispatcher.single_heuristic;

import ch.ethz.matsim.av.config.AVDispatcherConfig;
import ch.ethz.matsim.av.data.AVOperator;
import ch.ethz.matsim.av.data.AVVehicle;
import ch.ethz.matsim.av.dispatcher.AVDispatcher;
import ch.ethz.matsim.av.dispatcher.AVVehicleAssignmentEvent;
import ch.ethz.matsim.av.dispatcher.utils.SingleRideAppender;
import ch.ethz.matsim.av.framework.AVModule;
import ch.ethz.matsim.av.passenger.AVRequest;
import ch.ethz.matsim.av.plcpc.LeastCostPathFuture;
import ch.ethz.matsim.av.plcpc.ParallelLeastCostPathCalculator;
import ch.ethz.matsim.av.schedule.AVDriveTask;
import ch.ethz.matsim.av.schedule.AVOptimizer;
import ch.ethz.matsim.av.schedule.AVStayTask;
import ch.ethz.matsim.av.schedule.AVTask;
import ch.ethz.matsim.av.generaterandom.*;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.matsim.api.core.v01.Coord;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.contrib.dvrp.data.Vehicle;
import org.matsim.contrib.dvrp.path.VrpPath;
import org.matsim.contrib.dvrp.path.VrpPathWithTravelData;
import org.matsim.contrib.dvrp.path.VrpPaths;
import org.matsim.contrib.dvrp.schedule.Schedules;
import org.matsim.contrib.dvrp.tracker.OnlineDriveTaskTracker;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.router.DijkstraFactory;
import org.matsim.core.router.costcalculators.OnlyTimeDependentTravelDisutility;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.router.util.LeastCostPathCalculator.Path;
import org.matsim.core.router.util.TravelTime;
import org.matsim.core.utils.collections.QuadTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SingleHeuristicDispatcher extends AVOptimizer implements AVDispatcher  {
    private boolean reoptimize = true;

    final private SingleRideAppender appender;
    final private Id<AVOperator> operatorId;
    final private EventsManager eventsManager;

    final private List<AVVehicle> availableVehicles = new LinkedList<>();
    final private List<AVRequest> pendingRequests = new LinkedList<>();
    final private HashSet<AVVehicle> routingInProgress = new HashSet<>();

    final private QuadTree<AVVehicle> availableVehiclesTree;
    final private QuadTree<AVRequest> pendingRequestsTree;

    final private Map<AVVehicle, Link> vehicleLinks = new HashMap<>();
    final private Map<AVRequest, Link> requestLinks = new HashMap<>();
    
    final private Network network;
    final private ParallelLeastCostPathCalculator router;
    final private TravelTime travelTime;
    final private int policy;
    final private LeastCostPathCalculator serialRouter;

    public enum HeuristicMode {
        OVERSUPPLY, UNDERSUPPLY
    }

    private HeuristicMode mode = HeuristicMode.OVERSUPPLY;

    public SingleHeuristicDispatcher(Id<AVOperator> operatorId, EventsManager eventsManager, Network network, SingleRideAppender appender,
    		ParallelLeastCostPathCalculator router, TravelTime travelTime, int policy, LeastCostPathCalculator serialRouter) {
        this.appender = appender;
        this.operatorId = operatorId;
        this.eventsManager = eventsManager;
        this.network = network;
        this.router = router;
        this.travelTime = travelTime;
        // 1 for random
        // 2 for random but close
        // 3 for wait and then go
        // 4 for center of the city
        this.policy = policy;
        this.serialRouter = serialRouter;

        double[] bounds = NetworkUtils.getBoundingBox(network.getNodes().values()); // minx, miny, maxx, maxy

        availableVehiclesTree = new QuadTree<>(bounds[0], bounds[1], bounds[2], bounds[3]);
        pendingRequestsTree = new QuadTree<>(bounds[0], bounds[1], bounds[2], bounds[3]);
    }
    
    @Override
    public void onRequestSubmitted(AVRequest request) {
        addRequest(request, request.getFromLink());
    }
    
    public static double euclideanDistance(double x1, double y1, double x2, double y2) {   
    	   return Math.sqrt((x1 - x2)*(x1 - x2) + (y1 - y2)*(y1 - y2));  
    }
    
    @Override
    public void onNextTaskStarted(AVVehicle vehicle) {
    	    	
    	AVTask task = (AVTask)vehicle.getSchedule().getCurrentTask();
    	
    	if (task.getAVTaskType() == AVTask.AVTaskType.PICKUP) {
    		routingInProgress.remove(vehicle);
    	}
    	
        if (task.getAVTaskType() == AVTask.AVTaskType.STAY) {
        		if (!routingInProgress.contains(vehicle)) {
        			if (policy == 4) {
        				if (!availableVehicles.contains(vehicle)) {
        					addVehicle(vehicle, ((AVStayTask) task).getLink());
        				}
        				// find city center
        				task.setEndTime(task.getBeginTime());
        				
        				double[] bounds = NetworkUtils.getBoundingBox(network.getNodes().values());        					
        				Link startLink = ((AVStayTask) task).getLink();
        				double now = task.getBeginTime();
        				
        				double cityCenterX = (bounds[0] + bounds[2]) / 2;
        				double cityCenterY = (bounds[1] + bounds[3]) / 2;
        				Coord cityCenter = new Coord(cityCenterX, cityCenterY);
        				Link endLink = NetworkUtils.getNearestLink(network, cityCenter);
        				Random rn = new Random();
        				VrpPathWithTravelData pickupPath = null;
        				int indexx=2;
        				int indexy=2;
        				do {
        					try {
        						cityCenterX = (bounds[0] + bounds[2]) / indexx;
                				cityCenterY = (bounds[1] + bounds[3]) / indexy;
                				cityCenter = new Coord(cityCenterX + rn.nextInt(100), cityCenterY + rn.nextInt(100));

                				endLink = NetworkUtils.getNearestLink(network, cityCenter);

                				synchronized (serialRouter) {
        							pickupPath = VrpPaths.calcAndCreatePath(startLink, endLink, now, serialRouter, travelTime);
        						}
        					}
        					catch (NullPointerException e) {
        						continue;
        					}
        				}while (pickupPath == null);
        				
        				AVDriveTask fakeDriveTask = new AVDriveTask(pickupPath);
        				vehicle.getSchedule().addTask(fakeDriveTask);
        				AVStayTask fakeStayTask = new AVStayTask(fakeDriveTask.getEndTime(), 208000, endLink);
        				vehicle.getSchedule().addTask(fakeStayTask);
        			}
        		}
        }
    }
    
    @Override
    public void vehicleEnteredNextLink(Vehicle vehicle, Link link) {
    		AVTask task = (AVTask)vehicle.getSchedule().getCurrentTask();
    		if (task.getAVTaskType() == AVTask.AVTaskType.DRIVE) {
    			if (((AVDriveTask) task).isUnoccupied() && availableVehicles.contains(vehicle)) {
    				removeVehicle((AVVehicle) vehicle);
    				addVehicle((AVVehicle)vehicle, link);
    				availableVehiclesTree.put(link.getCoord().getX(), link.getCoord().getY(), (AVVehicle)vehicle);		
    			}
    			
    			if  (((AVDriveTask) task).isUnoccupied() && !availableVehicles.contains(vehicle)) {
    				addVehicle((AVVehicle)vehicle, link);
    			}
    			
    		}
    		
    		
    		
    }
    
    private void reoptimize(double now) {
        HeuristicMode updatedMode = availableVehicles.size() > pendingRequests.size() ? HeuristicMode.OVERSUPPLY : HeuristicMode.UNDERSUPPLY;

        if (!updatedMode.equals(mode)) {
            mode = updatedMode;
            eventsManager.processEvent(new ModeChangeEvent(mode, operatorId, now));
        }

        while (pendingRequests.size() > 0 && availableVehicles.size() > 0) {
            AVRequest request = null;
            AVVehicle vehicle = null;

            switch (mode) {
                case OVERSUPPLY:
                    request = findRequest();
                    vehicle = findClosestVehicle(request.getFromLink());
                    break;
                case UNDERSUPPLY:
                    vehicle = findVehicle();
                    request = findClosestRequest(vehicleLinks.get(vehicle));
                    break;
            }
            
            AVTask task = (AVTask)vehicle.getSchedule().getCurrentTask();
    		if (task.getAVTaskType() == AVTask.AVTaskType.DRIVE) {
    			if (((AVDriveTask) task).isUnoccupied() && availableVehicles.contains(vehicle)) {
    	            AVDriveTask dt = (AVDriveTask) task;
    	            OnlineDriveTaskTracker tracker = (OnlineDriveTaskTracker) dt.getTaskTracker();
    	            VrpPathWithTravelData pickupPath = null;
    	            try {
    	            	synchronized( serialRouter ) {
    	            	pickupPath = VrpPaths.calcAndCreatePath(tracker.getDiversionPoint().link, tracker.getDiversionPoint().link,
            	            		tracker.getDiversionPoint().time, serialRouter, travelTime);
    	            	}
    	            } 
    	            catch (NullPointerException e) {
						continue;
					}
    	            
    	            while (pickupPath == null) {
    	            	removeVehicle(vehicle);
    	            	vehicle = findClosestVehicle(request.getFromLink());
    	            	task = (AVTask)vehicle.getSchedule().getCurrentTask();
    	            	if (task.getAVTaskType() == AVTask.AVTaskType.DRIVE) {
    	        			if (((AVDriveTask) task).isUnoccupied() && availableVehicles.contains(vehicle)) {
    	        	            dt = (AVDriveTask) task;
    	        	            tracker = (OnlineDriveTaskTracker) dt.getTaskTracker();
    	        	            pickupPath = VrpPaths.calcAndCreatePath(tracker.getDiversionPoint().link, tracker.getDiversionPoint().link,
    	        	            		tracker.getDiversionPoint().time, serialRouter, travelTime); 
    	        			}
    	            	} else {
    	            		break;
    	            	}
    	            	tracker.divertPath(pickupPath);
        	            double endTime = vehicle.getSchedule().getEndTime();
        	            vehicle.getSchedule().removeLastTask();
        	            
        	            AVStayTask fakeStayTask = new AVStayTask(dt.getEndTime(), 208000, tracker.getDiversionPoint().link);
        	            vehicle.getSchedule().addTask(fakeStayTask);
    	            }
    	            
    			}
    		}
    		
            removeRequest(request);
            removeVehicle(vehicle);
            
            routingInProgress.add(vehicle);
            appender.schedule(request, vehicle, now);
        }
    }

    @Override
    public void onNextTimestep(double now) {
        appender.update();
        //routingInProgress.clear();
        if (reoptimize) reoptimize(now);
    }

    private void addRequest(AVRequest request, Link link) {
        pendingRequests.add(request);
        pendingRequestsTree.put(link.getCoord().getX(), link.getCoord().getY(), request);
        requestLinks.put(request, link);
        reoptimize = true;
    }

    private AVRequest findRequest() {
        return pendingRequests.get(0);
    }

    private AVVehicle findVehicle() {
        return availableVehicles.get(0);
    }

    private AVVehicle findClosestVehicle(Link link) {
        Coord coord = link.getCoord();
        return availableVehiclesTree.getClosest(coord.getX(), coord.getY());
    }

    private AVRequest findClosestRequest(Link link) {
        Coord coord = link.getCoord();
        return pendingRequestsTree.getClosest(coord.getX(), coord.getY());
    }

    @Override
    public void addVehicle(AVVehicle vehicle) {
        eventsManager.processEvent(new AVVehicleAssignmentEvent(vehicle, 0));
        addVehicle(vehicle, vehicle.getStartLink());
    }

    private void addVehicle(AVVehicle vehicle, Link link) {
        availableVehicles.add(vehicle);
        availableVehiclesTree.put(link.getCoord().getX(), link.getCoord().getY(), vehicle);
        vehicleLinks.put(vehicle, link);
        reoptimize = true;
    }

    private void removeVehicle(AVVehicle vehicle) {
        availableVehicles.remove(vehicle);
        Coord coord = vehicleLinks.remove(vehicle).getCoord();
        availableVehiclesTree.remove(coord.getX(), coord.getY(), vehicle);
    }

    private void removeRequest(AVRequest request) {
        pendingRequests.remove(request);
        Coord coord = requestLinks.remove(request).getCoord();
        pendingRequestsTree.remove(coord.getX(), coord.getY(), request);
    }

    static public class Factory implements AVDispatcherFactory {
        @Inject @Named(AVModule.AV_MODE)
        private Network network;

        @Inject private EventsManager eventsManager;

        @Inject @Named(AVModule.AV_MODE)
        private TravelTime travelTime;

        @Inject @Named(AVModule.AV_MODE)
        private ParallelLeastCostPathCalculator router;

        @Override
        public AVDispatcher createDispatcher(AVDispatcherConfig config) {
        		LeastCostPathCalculator serialRouter = new DijkstraFactory().createPathCalculator(
        				network, new OnlyTimeDependentTravelDisutility(travelTime), travelTime);
            return new SingleHeuristicDispatcher(
                    config.getParent().getId(),
                    eventsManager,
                    network,
                    new SingleRideAppender(config, router, travelTime),
                    router,
                    travelTime,
                    4,
                    serialRouter
            );
        }
    }
}
