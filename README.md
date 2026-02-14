
Apache Hadoop MapReduce & Apache Spark

### Student Name: Prashant Kumar Mishra

### Roll Number: M25DE1063


### Assignment: 1


---

## Overview

This assignment focuses on understanding and implementing **Apache Hadoop MapReduce** concepts using the classic **WordCount** example, followed by analytical tasks using **Apache Spark** (as per assignment instructions).

The Hadoop section involves:

* Writing custom `map()` and `reduce()` functions
* Compiling and executing MapReduce jobs
* Running WordCount on input datasets stored in **HDFS**
* Verifying correctness using output files and counters

---

## System Setup

* **Operating System**: Linux (Ubuntu / WSL)
* **Java Version**: JDK 8+
* **Apache Hadoop**: Single Node Cluster
* **Execution Mode**: Local / Pseudo-distributed
* **Language**: Java (MapReduce)

---

## Project Structure

```
wc/
├── WordCount.java
├── WordCount.jar
├── README.md
├── wc-input/
│   └── song.txt
└── output.txt
```

---

## Hadoop – WordCount Implementation

###  Question 1

Successfully ran the WordCount example as demonstrated in the official Hadoop MapReduce tutorial.
Terminal output screenshots are included in the submission PDF.

---

###  Question 2 – Map Phase Analysis

* **Input Key Type**: `LongWritable` (byte offset)
* **Input Value Type**: `Text` (line of text)
* **Output Key Type**: `Text` (word)
* **Output Value Type**: `IntWritable` (count = 1)

---

### Question 3 – Reduce Phase Analysis

* **Input Key**: `Text` (word)
* **Input Value**: `Iterable<IntWritable>`
* **Output Key**: `Text`
* **Output Value**: `IntWritable` (total count)

---

###  Question 4 – Mapper, Reducer & Job Configuration

* Mapper Class: `TokenizerMapper`
* Reducer Class: `IntSumReducer`
* Output Key Class: `Text.class`
* Output Value Class: `IntWritable.class`

---

###  Question 5 – `map()` Function

The `map()` function:

* Converts text to lowercase
* Removes punctuation
* Tokenizes input lines using `StringTokenizer`
* Emits `(word, 1)` pairs

The code was compiled successfully with no errors.

---

###  Question 6 – `reduce()` Function

The `reduce()` function:

* Iterates over values for each key
* Sums occurrences of each word
* Writes final `(word, count)` pairs to context

---

## Compilation Steps

```bash
javac -classpath $(hadoop classpath) -d . WordCount.java
jar -cvf WordCount.jar *
```

Compilation completed successfully without errors.

---

##  Input Data (HDFS)

```bash
hadoop fs -mkdir -p /user/hadoop/wc-input
hadoop fs -copyFromLocal song.txt /user/hadoop/wc-input/
```

---

##  Execution Command

```bash
hadoop jar WordCount.jar WordCount /user/hadoop/wc-input/song.txt output
```

Successful execution confirmed with:

```
INFO mapreduce.Job: Job completed successfully
```

---

##  Output Retrieval

```bash
hadoop fs -getmerge output output.txt
```

### Sample Output Format

```
all     4
night   4
up      4
we      4
```

---

##  Observations

* Hadoop MapReduce processes input strictly from **HDFS**
* Correct Mapper/Reducer class binding in `main()` is critical
* `job.waitForCompletion(true)` is mandatory for execution
* Output directory is created only on successful job completion

---

##  Submission Notes

* Screenshots of compilation, execution, and output are included in the PDF
* Code is typed manually and follows Hadoop API conventions
* No external code was copied directly

---


