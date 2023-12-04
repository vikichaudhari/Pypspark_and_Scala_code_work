// Databricks notebook source

val dd=5

// COMMAND ----------

var dd=666

// COMMAND ----------

var first=0
println(first)
var second=1
println(second)
for(x<-1 to 10){
val fib=first+second
println(fib)
first=second
second=fib
}

// COMMAND ----------

def trans(x :Int, f: Int =>Int):Int={f(x)}

// COMMAND ----------

print(trans(2,x=>x*9))

// COMMAND ----------

def hello() = println("Hello, World!")

// COMMAND ----------

import scala.io.StdIn.readLine

def helloInteractive() =
  println("Please enter your name:")
  val name = readLine()

  println("Hello, " + name + "!")

// COMMAND ----------

val names = List("chris", "ed", "maurice")
val capNames = for (name <- names) yield name.capitalize

// COMMAND ----------

var x = 1

while (x < 3) {
  println(x)
  x += 1
}

// COMMAND ----------

val name = "James"
val age = 30
println(s"$name is $age years old")

// COMMAND ----------

if x == 1 then
  println("x is 1, as you can see:")
  println(x)
else
  println("x was not 1")

// COMMAND ----------

sc=spark.SprkContext()

// COMMAND ----------

