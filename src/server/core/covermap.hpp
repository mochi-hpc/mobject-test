/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef COVERAGE_MAP
#define COVERAGE_MAP

#include <iostream>
#include <list>
#include <map>

template <typename T> class covermap {

    const T        m_start;
    const T        m_end;
    T              m_level;
    std::map<T, T> m_segments;

    static bool
    intersects(const T& start1, const T& end1, const T& start2, const T& end2)
    {
        if ((start1 == end1) || (start2 == end2)) return false;
        if (start1 == start2) return true;
        if (start1 < start2) {
            return start2 < end1;
        } else {
            return start1 < end2;
        }
    }

  public:
    struct segment {
        T start;
        T end;
        segment() {}
        segment(T s, T e) : start(s), end(e) {}
    };

    covermap(T s, T e) : m_start(s), m_end(e), m_level(0) {}

    std::list<segment> set(T start, T end)
    {
        // make start and end match the bounds
        if (start < m_start) start = m_start;
        if (end > m_end) end = m_end;
        if (start >= m_end) return std::list<segment>();
        if (end <= m_start) return std::list<segment>();

        if (end - start == 0) return std::list<segment>();

        std::list<segment> result;
        // easy case: the coverage map is empty
        if (m_segments.empty()) {
            m_segments[start] = end;
            result.emplace_back(start, end);
            m_level += (end - start);
            return result;
        }

        // not easy case
        // find the first segment intersecting
        auto first_seg = m_segments.lower_bound(start);
        // we need to go back left
        if (first_seg != m_segments.begin()) first_seg--;
        // check if the first segment we found intersects
        if (!intersects(first_seg->first, first_seg->second, start, end)) {
            // it does not intersect
            first_seg++;
        }

        // find the first segment on the right that does NOT intersect
        auto last_seg = m_segments.lower_bound(end);
        // if the first_seg and last_seg are the same, then we can
        // insert the new segment directly
        if (first_seg == last_seg) {
            result.emplace_back(start, end);
            m_level += (end - start);
            m_segments[start] = end;
            return result;
        }
        // otherwise, we iterate to build the list
        auto it = first_seg;
        if (first_seg->first > start)
            result.emplace_back(start, first_seg->first);
        for (; it != last_seg; it++) {
            auto jt = it;
            jt++;
            if (jt != last_seg) {
                result.emplace_back(it->second, jt->first);
                m_level += (jt->first - it->second);
            } else if (it->second < end) {
                result.emplace_back(it->second, end);
                m_level += (end - it->second);
            }
        }
        start            = std::min(first_seg->first, start);
        auto before_last = last_seg;
        before_last--;
        end = std::max(before_last->second, end);
        m_segments.erase(first_seg, last_seg);
        m_segments[start] = end;
        return result;
    }
    void print(std::ostream& ostr)
    {
        for (auto& s : m_segments) {
            ostr << "[" << s.first << "," << s.second << "[";
        }
    }

    T level() const { return m_level; }

    T capacity() const { return (m_end - m_start); }

    bool full() const { return capacity() == level(); }

    uint64_t bytes_read() const
    {
        if (full()) return capacity();
        if (m_segments.empty()) return 0;
        uint64_t start = m_end;
        uint64_t end   = m_start;
        for (const auto& s : m_segments) {
            if (s.first < start) start = s.first;
            if (s.second > end) end = s.second;
        }
        return end - start;
    }
};

#endif
