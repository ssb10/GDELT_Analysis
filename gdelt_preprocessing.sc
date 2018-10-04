	// Working with events file
    // data stored on HDFS
    val events = sc.textFile(args[1]+"/*.CSV")

    val ind = List(0, 1, 5, 6, 7, 15, 16, 17, 25, 26, 30, 31, 34, 39, 40, 46, 47, 57)

    val events_selected = events.map(line => line.split("\t")).map(line => ind.map(line))

    val events_filtered = events_selected.filter(line => line(9).startsWith("01")).filter(line => line(4).length!=0).filter(line => line(4).equals("USA"))


    val event_cols = "EventID Date Actor1Code Actor1_Name Actor1_CountryCode Actor2Code Actor2Name Actor2CountryCode IsRootEvent EventCode GoldSteinScale NumMentions AvgTone SourceURL"
    val schema = StructType(event_cols.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = events_filtered.map(line =>Row(line(0),line(1),line(2),line(3),line(4),line(5),line(6),line(7),line(8),line(9),li ne(10),line(11),line(12),line(13)))
    val gdeltDF = sqlCtx.createDataFrame(rowRDD, schema)

    gdeltDF.coalesce(1).write.json(args[3])


    // working with mentions file

    val mentions = sc.textFile(args[2]+"/*.CSV")

    val ind = List(0,1,2,3,7,8,9,12)
    val gdelt_mentions_selected = gdelt_mentions.map(line => line.split("\t")).map(line => ind.map(line))

    val mention_cols = "EventID EventDateTime MentionDateTime MentionType Actor1Offset Actor2Offset ActionOffset MentionDocTone"

    val rowRDD = gdelt_mentions_selected.map(line => Row(line(0),line(1),line(2),line(3),line(4),line(5),line(6),line(7)))

    val gdeltmentionsDF = sqlCtx.createDataFrame(rowRDD, schema)

    // Working with GKG Files

    val gkg = sc.textFile(args[0]+"/*.gkg.csv.zip")
    val total = gkg.map(line => line.split("\t")).filter(t => t.length > 2).filter(line => line(1).length>8).map(line => line.map(_.replace(line(1),line(1).substring(0,8)))).map(line => line(1))
    val df1 = total.toDF("Date")
    val df2 = df1.groupBy(col("Date")).agg(
      count("Date").as("totalart")
    ).orderBy(col("Date"))
    val gkg_selected = gkg.map(line => line.split("\t")).filter(t => t.length ==25).map(t => Array(t(0), t(1), t(7), t(9),t(15).split(",")(0)))
    val gkg_keyvalue = gkg_selected.filter(line => line(3).length!=0).filter(line => line(2).startsWith("ECON_")).filter(line => line(1).length>8).map(line => line.map(_.replace(line(1),line(1).substring(0,8)))).map(line => (line(3).split("#")(2),line(1),line(4)))
    val df3 = gkg_keyvalue.toDF("Loc","Date","Tone")
    val dftemp = df3.withColumn("aggtone",$"Tone".cast(FloatType)).drop("Tone")
    val df4 = dftemp.groupBy(col("Loc"),col("Date")).agg(
      avg("aggtone").as("AvgTone"),
      count("aggtone").as("art")
    ).orderBy(col("Loc"),col("Date"))
    val df5 = df4.join(df2,"Date")
    val df6 = df5.withColumn("normalized",$"AvgTone" * $"art"*1000 / $"totalart").drop("AvgTone","art","totalart").orderBy(col("Loc"),col("Date"))
    val df7 = dftemp.groupBy(col("Date")).agg(
      avg("aggtone").as("AvgTone"),
      count("aggtone").as("totart")
    ).orderBy(col("Date")).drop("Loc")
    val df8 = df7.join(df2,"Date")
    val df9 = df8.withColumn("wti",$"AvgTone" * $"totart"*1000 / $"totalart").drop("AvgTone","totart","totalart").orderBy(col("Date"))
    df6.coalesce(1).write.csv("gkg/aprw3.csv")
    df9.coalesce(1).write.csv("gkg/wtiaprw3.csv")
    spark.stop()