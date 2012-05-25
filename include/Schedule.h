#ifndef SCHEDULE_H_
#define SCHEDULE_H_

/*!\file Schedule.h
 * \brief Schedule (key, value) pairs according to the LogicArrayBlocks
 *
 * To Do:
 * Currently we do not have any algorithm to do the scheduling. The goal of the
 * scheduling is to find <num_proc> lists for each node, so that following the
 * sequence of each list, the size of loading is minimum. One heuristic is to
 * find a block that has the minimum loading cost with respect to the current
 * local storage. However, even this heuristic is very complicated. With a even
 * special and simple case, such that each block is specified by only one
 * number, like A[0], B[1]. In that case we can use a simple sorting process to
 * solve it.
 */

namespace sched {

template<typename KeyType, typename ValType>
void genWorkList(std::map< std::pair<KeyType, ValType>, 
                 std::vector<LogicArrayBlocks> > map, 
                 std::vector< std::pair<KeyType, ValType> >& work_list);

} /* namespace sched */

#endif /* SCHEDULE_H_ */
