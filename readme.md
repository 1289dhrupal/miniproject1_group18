# Mini project 1 sample

This project is based on the project template of the worksheet for week 13 and it provides a couple of examples of acceptable queries for the mini project using the IMDB movie dataset used in the programming exercises:

## Query 1

This is a simple query, aiming to a satisfactory mark.

  * Query: Select movies with more than a 9 rating and project title and year. Then the resulting movies are sorted by year, in ascending order. 
  * Groovy implementation: `src/main/groovy/Query_Groovy_simple.groovy`
  * MongoDB implementation: `src/main/groovy/Query_MongoDB_simple.groovy`

## Query 2

This is a more compelling query exercising more features of both Groovy and MongoDB and it should therefore lead to a better mark.

  * Query: Select actors that star more than 1 movie with at least a 9 imdb rating.
  * Groovy implementation: `src/main/groovy/Query_Groovy_compelling.groovy`
  * MongoDB implementation: `src/main/groovy/Query_Groovy_compelling.groovy`


## Ideas for the analysis in exercise 4

In addition to the rubric you may want to use some of the questions below to discuss exercise 4. This is just a sample of questions, which is not supposed to be prescriptive. Feel free to choose and expand according to your interests.

### Evolution

* Which implementation is more understandable? Why?
* Which implementation is likely to be more maintainable? Why?
* When importing movies from other datasets:
  * What is the impact of a change of data structure in the query evaluation?
  * Would you need to develop a new database? Would you need to migrate data to the new version? Why? How?
  * Are there any implications regarding forward/backward compatibility?

### Scalability

* Which implementation is faster? Have you measured it? How?
* Which implementation can be scaled? How? 
* Which implementation cannot be scaled? Why not?

Here the point is to do some research on the support that MongoDb offers for managing clusters.

### Consistency

* If you have different applications modifying the same dataset (stored as a JSON file or in the MongoDB database), how would you ensure that there are no write-write conflicts? Here the point is to discuss how concurrency can be handled in Groovy and in MongoDB.
* If the dataset is replicated, how would you ensure that there are no write-write conflicts? and write-read conflicts? Here the point is to do some research on the support that MongoDb offers for managing clusters, including the notion of write concern.


## Changing the name of the project

Go to `build.gradle` and change the name in `eclipse.project.name` as shown in the code snipped below (`...` refers to additional code that has been removed for the sake of presentation):

```
...
eclipse {
	project {
		name 'week14_miniproject1_sample'
		...
	}
}
...
```





***
&copy; Artur Boronat, 2015-20
