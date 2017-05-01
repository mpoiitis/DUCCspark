package com.poiitis.pli;

import java.io.Serializable;

/**
 *
 * @author Poiitis Marinos
 */
public class IntPair implements Serializable, Comparable<IntPair> {

  private static final long serialVersionUID = 2000453531470227564L;

  private int first;
  private int second;

  public IntPair(int first, int second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (first ^ (first >>> 32));
    result = prime * result + (int) (second ^ (second >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    IntPair other = (IntPair) obj;
    if (first != other.first) {
      return false;
    }
    return second == other.second;
  }

  public int getFirst() {
    return this.first;
  }

  public void setFirst(int first) {
    this.first = first;
  }

  public int getSecond() {
    return this.second;
  }

  public void setSecond(int second) {
    this.second = second;
  }

  @Override
  public int compareTo(IntPair other) {
    if (other == null) {
      return 1;
    }

    if (other.first == this.first) {
      return (int) (this.second - other.second);
    }
    return (int) (this.first - other.first);
  }


}
