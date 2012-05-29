#include "LogicArray.h"

/*** Range ***/
/* ctor/dtor */
inline Range::Range(const unsigned& start, const unsigned& span) 
  : m_start(start), m_end(start + span) { }

inline Range::Range(const unsigned& pos)
  : m_start(pos), m_end(pos) { }

/* getter/setter */
inline const unsigned& start() const { return m_start; }
inline void start(const unsigned& start) { m_start = start; }
inline const unsigned& end() const { return m_end; }
inline const unsigned& span() const { return m_end - m_start + 1; }
inline void span(const unsigned& span) { m_end = m_start + span - 1; }

/* helper function */
inline unsigned min(unsigned v0, unsigned v1) {
  return (v0 < v1) ? v0 : v1;
}

inline unsigned max(unsigned v0, unsigned v1) {
  return (v0 > v1) ? v0 : v1;
}

/* hasOverlapWith() */
inline bool hasOverlapWith(const Range& other) {
  if (m_end < other.start()) return false;
  if (m_start > other.end()) return false;
  return true;
}

/* contains() */
inline bool contains(const Range& other) {
  if (m_start > other.start()) return false;
  if (m_end < other.end()) return false;
  return true;
}

/* intersect() */
inline Range Range::intersect(const Range& other) {
  unsigned start = max(other.start(), m_start);
  unsigned end = min(other.end(), m_end);
  return Range(start, end - start + 1);
}

/* include() */
inline Range Range::include(const Range& other) {
  unsigned start = min(other.start(), m_start);
  unsigned end = max(other.end(), m_end);
  return Range(start, end - start + 1);
}

/* exclude() */
inline Range Range::exclude(const Range& other) {
  unsigned start, end;
  if (m_start < other.start()) {
    start = m_start;
    end = other.start();
  }
  else {
    start = m_end;
    end = other.end();
  }
  return Range(start, end - start + 1);
}

/*** LogicArrayBlock ***/
/* ctor/dtor */
inline LogicArrayBlock::LogicArrayBlock() :
    m_array(NULL), m_ranges() { }

inline LogicArrayBlock::LogicArrayBlock(
    const LogicArray& array,
    const std::vector<Range>& ranges) :
    m_array(&array), m_ranges(ranges) { }

/* intersect */
inline LogicArrayBlock LogicArrayBlock::intersect(
    const LogicArrayBlock& other) {
  //no intersection for different logic array
  if (other.array() != m_array) return LogicArrayBlock();

  //get intersection for each dimension

}


