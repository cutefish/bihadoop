#include "LogicArray.h"

/*** Range ***/
/* ctor/dtor */
inline Range::Range() { }

inline Range::Range(const unsigned& start, const unsigned& end) 
  : m_start(start), m_end(end) { }

inline Range::Range(const unsigned& pos)
  : m_start(pos), m_end(pos) { }

/* getter/setter */
inline const unsigned& start() const { return m_start; }
inline void start(const unsigned& start) { m_start = start; }
inline const unsigned& end() const { return m_end; }
inline void end(const unsigned& end) { m_end = end; }

/*** Loader ***/

