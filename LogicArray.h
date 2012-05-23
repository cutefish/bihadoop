#ifndef LOGICARRAY_H_
#define LOGICARRAY_H_

#include <vector>

/*!\file LogicArray.h
 * \brief This file implements a abstract LogicArray data structure. The
 * instances of LogicArray can be very large such that it is possible that no
 * single storage unit is large enough to store it. Methods are provided to load
 * a part of the LogicArray into local storage unit to access it.
 */

namespace logic_array {

static const EntireDim = -1;

} /* logic_array */

/*!\class Range
 * \brief Represent an unsigned interval [start(), end()].
 */
class Range {
 public:
  /*** ctor/dtor ***/
  Range();
  Range(const unsigned& start, const unsigned& end);
  Range(const unsigned& pos);
  //use default copy, assignment ctor, and dtor

  /*** getter/setter ***/
  const unsigned& start() const;
  void start(const unsigned& start);
  const unsigned& end() const;
  void end(const unsigned& end);

};

/*!\class Loader
 * \brief Loader for LogicArray. LogicArray uses this class to load a block of
 * data into local storage unit.
 */
class Loader {
};

/*!\class DefaltLoader
 * \brief DefaultLoader simply new a block on heap 
 */
class DefaultLoader : public Loader {
};

/*!\class Dumper
 * \brief Dumper for LogicArray. LogicArray uses this class to dump a block of
 * data from local storage unit to disk.
 */
class Dumper {
};

/*!\class LogicArray
 */
class LogicArray {
};

#endif /* LOGICARRAY_H_ */
