# B+ Tree Cpp


## Description
Implementation of a dynamic multilevel tree-based index.

## Compatibility

- C++ 20 or higher

## About B+ Trees
In a Clustered B+ Tree, records are stored _only at the leaf nodes_ of the tree. The leaf nodes are also linked to provide 
ordered access to the records when performing a search operation. These nodes can be interpreted as the deepest (base) level of the index.

Internal nodes of the B+ Tree correspond to the superior levels of the multilevel index. These nodes helps to determine the 
descend criteria when performing the index operations.



The structure of the _internal nodes_ of a B+ tree with _index page capacity_ $M$ is as follows:
 
 - Each _internal node_ is of the form $\left[P_1, K_1, P_2, K_2, ..., P_{q-1}, K_{q-1}, P_q \right]$, where $q \leq M$. Each $K_i$ refers to a _key_ (for $1 \leq i \lt q$) and each $P_i$ refers to a _pointer to child node_ (for $1 \leq i \leq q$), the pointer may point either to a _internal node_ or to a _leaf node_. Note that an _internal node_ with $q$ _pointers_ has $q - 1$ _keys_.

 - Within each internal node we have that $K_{1} < K_{2} < ... < K_{q-1}$.

 - For all _key values_ $X$ in the subtree pointed by $P_{i}$, we have that:
   - $K_{i-1} \lt X \lt K_{i}$ for $1 \lt i \lt q$
   - $X \lt K_{i}$ for $i = 1$
   - $K_{i-1} \lt X$ for $i = M$

 
 - Each internal node has at least $\lceil \frac{M}{2} \rceil$ _children_ and at most $M$ _children_. The _root node_ has at least two _children_ if it is an _internal node_.

The structure of the _leaf nodes_ of a B+ tree with _data page capacity_ $N$ is as follows:

 - Each _leaf node_ is of the form $(P_{prev}, \left[R_1, R_2, ..., R_{q-1}, R_{q} \right], P_{next})$, where $q \leq N$. Each $R_i$ refers to a _record_ (for $1 \leq i \leq N$). $P_{prev}$ points to the previous _leaf node_ of the B+ Tree, same idea with $P_{next}$. 

 - Within each _leaf node_ we have that $K(R_{1}) < K(R_{2}) < ... < K(R_{q})$.

 - Each leaf node has at least $\lceil \frac{N}{2} \rceil$ _records_ and at most $N$ _records_. The _root node_ may have less than $\lceil \frac{N}{2} \rceil$ _records_ if it is the only node in the tree and only the _root node_ has this exception.
   
 - All _leaf nodes_ are at the same level.

## References

- [1] Elmasri, R. & Navathe, S. (2010). Indexing Structures for Files. In _Fundamentals of Database Systems_ (pp. 652-653). Addison–Wesley.
