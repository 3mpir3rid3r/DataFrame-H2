# DataFrame-H2
Java data frame and visualization library. created using the h2 database.

jFreechart view, using data frame data with calculated support and resistance.
![image_2021-12-15_225010](https://user-images.githubusercontent.com/35126851/146234116-1172e53c-9866-46c2-b62c-6096fd5d186b.png)

Print data in the console.
```
dataFrame.printAll();
```
![image_2021-12-15_225649](https://user-images.githubusercontent.com/35126851/146235192-e94f13fe-13b9-4889-b384-f84d41a1817f.png)

Print selected data in the console.
```
dataFrame.select(ColumnsNamesEnum.CANDLE_INDEX.toString(),ColumnsNamesEnum.DATE_AND_TIME.toString(),ColumnsNamesEnum.OPEN.toString()).printAll();
```
![image_2021-12-15_230243](https://user-images.githubusercontent.com/35126851/146236194-665279f5-9640-45ba-b766-5c548eb58306.png)

You can run any queries like this.
```
dataFrame.select(ColumnsNamesEnum.OPEN.toString()).where(Selection.create(ColumnsNamesEnum.OPEN.toString(), WhereConditionEnum.GREATER_THAN,40000)).groupBy(ColumnsNamesEnum.OPEN.toString()).executeForData();
```
![image_2021-12-15_230716](https://user-images.githubusercontent.com/35126851/146236801-182f1f35-5d98-42ff-bf80-2a9657aa2baa.png)

**Do not hesitate to get and improve, be kind enough to share it.**
