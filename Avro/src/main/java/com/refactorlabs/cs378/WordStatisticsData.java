/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class WordStatisticsData extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"WordStatisticsData\",\"namespace\":\"com.refactorlabs.cs378\",\"fields\":[{\"name\":\"document_count\",\"type\":\"long\"},{\"name\":\"total_count\",\"type\":\"long\"},{\"name\":\"sum_of_squares\",\"type\":\"long\"},{\"name\":\"mean\",\"type\":[\"double\",\"null\"]},{\"name\":\"variance\",\"type\":[\"double\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public long document_count;
  @Deprecated public long total_count;
  @Deprecated public long sum_of_squares;
  @Deprecated public java.lang.Double mean;
  @Deprecated public java.lang.Double variance;

  /**
   * Default constructor.
   */
  public WordStatisticsData() {}

  /**
   * All-args constructor.
   */
  public WordStatisticsData(java.lang.Long document_count, java.lang.Long total_count, java.lang.Long sum_of_squares, java.lang.Double mean, java.lang.Double variance) {
    this.document_count = document_count;
    this.total_count = total_count;
    this.sum_of_squares = sum_of_squares;
    this.mean = mean;
    this.variance = variance;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return document_count;
    case 1: return total_count;
    case 2: return sum_of_squares;
    case 3: return mean;
    case 4: return variance;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: document_count = (java.lang.Long)value$; break;
    case 1: total_count = (java.lang.Long)value$; break;
    case 2: sum_of_squares = (java.lang.Long)value$; break;
    case 3: mean = (java.lang.Double)value$; break;
    case 4: variance = (java.lang.Double)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'document_count' field.
   */
  public java.lang.Long getDocumentCount() {
    return document_count;
  }

  /**
   * Sets the value of the 'document_count' field.
   * @param value the value to set.
   */
  public void setDocumentCount(java.lang.Long value) {
    this.document_count = value;
  }

  /**
   * Gets the value of the 'total_count' field.
   */
  public java.lang.Long getTotalCount() {
    return total_count;
  }

  /**
   * Sets the value of the 'total_count' field.
   * @param value the value to set.
   */
  public void setTotalCount(java.lang.Long value) {
    this.total_count = value;
  }

  /**
   * Gets the value of the 'sum_of_squares' field.
   */
  public java.lang.Long getSumOfSquares() {
    return sum_of_squares;
  }

  /**
   * Sets the value of the 'sum_of_squares' field.
   * @param value the value to set.
   */
  public void setSumOfSquares(java.lang.Long value) {
    this.sum_of_squares = value;
  }

  /**
   * Gets the value of the 'mean' field.
   */
  public java.lang.Double getMean() {
    return mean;
  }

  /**
   * Sets the value of the 'mean' field.
   * @param value the value to set.
   */
  public void setMean(java.lang.Double value) {
    this.mean = value;
  }

  /**
   * Gets the value of the 'variance' field.
   */
  public java.lang.Double getVariance() {
    return variance;
  }

  /**
   * Sets the value of the 'variance' field.
   * @param value the value to set.
   */
  public void setVariance(java.lang.Double value) {
    this.variance = value;
  }

  /** Creates a new WordStatisticsData RecordBuilder */
  public static com.refactorlabs.cs378.WordStatisticsData.Builder newBuilder() {
    return new com.refactorlabs.cs378.WordStatisticsData.Builder();
  }
  
  /** Creates a new WordStatisticsData RecordBuilder by copying an existing Builder */
  public static com.refactorlabs.cs378.WordStatisticsData.Builder newBuilder(com.refactorlabs.cs378.WordStatisticsData.Builder other) {
    return new com.refactorlabs.cs378.WordStatisticsData.Builder(other);
  }
  
  /** Creates a new WordStatisticsData RecordBuilder by copying an existing WordStatisticsData instance */
  public static com.refactorlabs.cs378.WordStatisticsData.Builder newBuilder(com.refactorlabs.cs378.WordStatisticsData other) {
    return new com.refactorlabs.cs378.WordStatisticsData.Builder(other);
  }
  
  /**
   * RecordBuilder for WordStatisticsData instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<WordStatisticsData>
    implements org.apache.avro.data.RecordBuilder<WordStatisticsData> {

    private long document_count;
    private long total_count;
    private long sum_of_squares;
    private java.lang.Double mean;
    private java.lang.Double variance;

    /** Creates a new Builder */
    private Builder() {
      super(com.refactorlabs.cs378.WordStatisticsData.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.refactorlabs.cs378.WordStatisticsData.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing WordStatisticsData instance */
    private Builder(com.refactorlabs.cs378.WordStatisticsData other) {
            super(com.refactorlabs.cs378.WordStatisticsData.SCHEMA$);
      if (isValidValue(fields()[0], other.document_count)) {
        this.document_count = data().deepCopy(fields()[0].schema(), other.document_count);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.total_count)) {
        this.total_count = data().deepCopy(fields()[1].schema(), other.total_count);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.sum_of_squares)) {
        this.sum_of_squares = data().deepCopy(fields()[2].schema(), other.sum_of_squares);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.mean)) {
        this.mean = data().deepCopy(fields()[3].schema(), other.mean);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.variance)) {
        this.variance = data().deepCopy(fields()[4].schema(), other.variance);
        fieldSetFlags()[4] = true;
      }
    }

    /** Gets the value of the 'document_count' field */
    public java.lang.Long getDocumentCount() {
      return document_count;
    }
    
    /** Sets the value of the 'document_count' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder setDocumentCount(long value) {
      validate(fields()[0], value);
      this.document_count = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'document_count' field has been set */
    public boolean hasDocumentCount() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'document_count' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder clearDocumentCount() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'total_count' field */
    public java.lang.Long getTotalCount() {
      return total_count;
    }
    
    /** Sets the value of the 'total_count' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder setTotalCount(long value) {
      validate(fields()[1], value);
      this.total_count = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'total_count' field has been set */
    public boolean hasTotalCount() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'total_count' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder clearTotalCount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'sum_of_squares' field */
    public java.lang.Long getSumOfSquares() {
      return sum_of_squares;
    }
    
    /** Sets the value of the 'sum_of_squares' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder setSumOfSquares(long value) {
      validate(fields()[2], value);
      this.sum_of_squares = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'sum_of_squares' field has been set */
    public boolean hasSumOfSquares() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'sum_of_squares' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder clearSumOfSquares() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'mean' field */
    public java.lang.Double getMean() {
      return mean;
    }
    
    /** Sets the value of the 'mean' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder setMean(java.lang.Double value) {
      validate(fields()[3], value);
      this.mean = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'mean' field has been set */
    public boolean hasMean() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'mean' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder clearMean() {
      mean = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'variance' field */
    public java.lang.Double getVariance() {
      return variance;
    }
    
    /** Sets the value of the 'variance' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder setVariance(java.lang.Double value) {
      validate(fields()[4], value);
      this.variance = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'variance' field has been set */
    public boolean hasVariance() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'variance' field */
    public com.refactorlabs.cs378.WordStatisticsData.Builder clearVariance() {
      variance = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public WordStatisticsData build() {
      try {
        WordStatisticsData record = new WordStatisticsData();
        record.document_count = fieldSetFlags()[0] ? this.document_count : (java.lang.Long) defaultValue(fields()[0]);
        record.total_count = fieldSetFlags()[1] ? this.total_count : (java.lang.Long) defaultValue(fields()[1]);
        record.sum_of_squares = fieldSetFlags()[2] ? this.sum_of_squares : (java.lang.Long) defaultValue(fields()[2]);
        record.mean = fieldSetFlags()[3] ? this.mean : (java.lang.Double) defaultValue(fields()[3]);
        record.variance = fieldSetFlags()[4] ? this.variance : (java.lang.Double) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
