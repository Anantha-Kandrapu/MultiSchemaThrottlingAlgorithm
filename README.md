### Problems solved

- Visibility into Contra's state in almost realtime.
- Automated priority based capacity allocation.
- Dynamic Capacity reallocation among schemas.
- Handling Cases where cross service and multiple schemas are overloaded.
- Automated back pressure mechanism.
- Automated dependency identification including loops.
- Minimize the entropy by reducing the number of changes.
- Reduce system complexity by taking iterative steps.
- Automated handling on common ancestor (meaning the services with a common root will have correct reduction in capacity).

### Discovery

- _Why slowlanes are necessary for any pipeline system

### NEXT UP

1. Identify the nodes where there are multiple downstream dependencies and add a path to slowlane because , slow down in one downstream dependency should not slow down the entire system
   Example : If hoth slowedDown in s1 doesn't mean it should backpressure at c1 and slowdown the even the bungee.
1. If all downstream dependencies are unable to handle traffic then slow down upstream.

### Pre Reqs

1. Need a way to quantify bungee's capacity.
1. Every service should emit input and output TPS metrics.
1. Every service's max and min capacity.

### Requiremnents for it to be beyond Concept
1. Pre Reqs
1. A better mechanism to calculate the back pressure factor.
1 .... 
