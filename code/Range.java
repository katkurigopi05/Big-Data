package com.hadoopbook.pig;

import java.util.ArrayList;
import java.util.List;

public class Range {
  private final int start;
  private final int end;

  public Range(int start, int end) {
    this.start = start;
    this.end = end;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public String getSubstring(String line) {
    return line.substring(start - 1, end);
  }

  public static List<Range> parse(String rangeSpec) throws IllegalArgumentException {
    if (rangeSpec.length() == 0) {
      return new ArrayList<Range>();
    }
    List<Range> ranges = new ArrayList<Range>();
    String[] specs = rangeSpec.split(",");
    for (String spec : specs) {
      String[] split = spec.split("-");
      try {
        ranges.add(new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    return ranges;
  }
}
