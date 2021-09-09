import sys
if __name__ == "__main__":
    words = sc.textFile("/FileStore/tables/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    wordCounts.saveAsTextFile("/FileStore/tables/livroWords.txt")
