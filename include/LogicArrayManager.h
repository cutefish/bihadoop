#ifndef INCLUDE_LOGICARRAYMANAGER_H_
#define INCLUDE_LOGICARRAYMANAGER_H_

#include "LogicArray.h"

/*!\file LogicArrayManager.h
 * \brief This file provide a storage management interface for the logic array.
 * Only blocks of the logic array can be inside a storage unit and thus this
 * interface is responsible for load and dump the blocks.
 */


/*!\class BlockManager
 * \brief Manage logic array blocks, check if it is in local storage, combine
 * blocks if necessary
 */
class BlockManager {
 public:
  /** ctor/dtor ***/
  BlockManager(unsigned num_dims);

  /*** member fucntions ***/
  /* getter/setter */
  const size_t& capacity() const;
  void capacity(const size_t& c);

  /* isLocal() */
  bool isLocal(std::vector<Range>);

  /* combine()
   * Combine ranges in the list to get a continuous address space for the
   * blocks.
   */
  void combine();

  /* diff()
   * Get a diff between \param Range and local so that we can bring a new block
   * having no intersection with the local ones.
   */
  std::vector<Range> diff(std::vector<Range>);

 private:
  std::list<Range> m_localblocks;
  unsigned m_numdims;
  size_t m_capacity;
};

#endif /* INCLUDE_LOGICARRAYMANAGER_H_ */
