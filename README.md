This service builds a stream of aggregated windows, using the key of the
inbound message to group by. 

Its aim is to be the simplest possible, tested window buildier. 

It will fail when the windows become too large for kafka. 
