# Stop canceled indexed relationship vector reads (NOR-75)

Observe caller cancellation while collecting and ranking indexed relationship vectors and while hydrating ranked hits. Keep healthy hit ordering and scores, and return the cancellation error without a partial result.
