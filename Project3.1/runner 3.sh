#! /bin/bash

######################################################################
# Answer script for Project 3 module 1 Fill in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# Qustion 1
# How many rows match 'Aerosmith' through the grep command?
# Run your commands/code to process the dataset and output a
# single number to standard output
answer_1() {grep -P 'Aerosmith' million_songs_metadata.csv | wc -l
    # Write a function to get the answer to Q1. Do not just echo the answer.
	
	# 180
}

# Qustion 2
# Write grep commands that result in the total number of track_id(s) with 
# artist_name containing "Bob Marley"
# Run your commands to process the dataset and output a single number 
# to standard output 
answer_2() {
    # Write grep commands to get the answer to Q2. Do not just echo the answer.
    cut -d, -f7 million_songs_metadata.csv | grep -P 'Bob Marley' | wc -l
    #cat m.csv | cut -d , -f 7 | grep "Bob Marley" | wc -l
    #awk ' BEGIN {FS = ","} ; {if ($7 ~/Bob Marley/) { print;}}' m.csv | wc -l
    # 184
}

# Qustion 3
# How many rows match 'The Beatles' through the awk command on column 7?
# The output should be a single number
answer_3() {
	# Write a function to get the answer to Q3. Do not just echo the answer.
	awk ' BEGIN {FS = ","} ; {if ($7 ~ /The Beatles/) { print; }}' million_songs_metadata.csv | wc -l
	#96
	awk 'BEGIN {FS = ","} {if($7 ~ /The Beatles/) sum += 1} END {print sum}' million_songs_metadata.csv
}

# Qustion 4
# How many tracks are composed from 1970 to 1989 (inclusive)
# by an artist whose artist_hotttness is above 0.8?
# The output should be a single number
answer_4() {
    # Write awk commands to get the answer to Q4. Do not just echo the answer.
    awk ' BEGIN {FS = ","} ; {if ($11 >= 1970 && $11 <= 1989 && $10 > 0.8) { print; }}' million_songs_metadata.csv | wc -l
    #171
    awk 'BEGIN {FS = ","} {if($11 >= 1970 && $11 <= 1989 && $10 > 0.8) sum+=1} END {print sum}' million_songs_metadata.csv
}

# Qustion 5
# Write awk code to do the equivalent of the SQL query SELECT
# AVG(duration) FROM songs;. The code should output a single number.
answer_5() {
	# Write a one-line command to get the answer to Q5. Do not just echo the answer.
	awk 'BEGIN {FS = ","} {sum += $8} END {print sum / NR}' million_songs_metadata.csv
	#249.501
}

# Qustion 6
# Invoke your shell script described in Part 1, step 5 of this project,
# How many songs are from Michael Jackson (case insensitive)?
answer_6() {
	# Write a function to get the answer to Q6. Do not just echo the answer.
	awk ' BEGIN {FS = ","} ; {if (tolower($7)=="michael jackson") { sum += 1 }}; END { print sum }' million_songs_metadata.csv
	#194
	grep -i -c "[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,michael jackson," million_songs_metadata.csv
}

# Qustion 7
# Invoke the awk / shell program or the set of commands that you wrote
# to merge the two files into the file million_songs_metadata_and_sales.csv
# in current folder.
answer_7() {
	# Write a function to get the answer to Q7. Do not just echo the answer.
	awk ' BEGIN {FS = ","} ; FNR==NR {a[$1]=substr($0,index($0,$2));next} {print $0 "," a[$1]}' million_songs_metadata.csv million_songs_sales_data.csv > million_songs_metadata_and_sales.csv
    join -t ',' -j1 1 -j2 1 million_songs_sales_data.csv million_songs_metadata.csv > million_songs_metadata_and_sales.csv
}

# Question 8 
# Find the artist with maximum sales using the file million_songs_metadata_and_sales.csv.
# The output of your command(s) should be the artist name.
answer_8() {
    # Write a function to get the answer to Q8. Do not just echo the answer.
    awk ' 
    BEGIN {FS = ","; max=0}  
    { artist_id[$7]+=$3 ; artist[$7]=$9 }
    END { for (id in artist_id){
        if (artist_id[id] > max){
            max = artist_id[id]
            n = id
            }
        }
    print artist[n],max}
    ' million_songs_metadata_and_sales.csv
    #Ike & Tina Turner
    awk ' 
    BEGIN {FS = ","; max = 0; n = 0}  
    {artistID[$7]+=$3 ; artist[$7]=$9}
    END {for(i in artistID){
        if(artistID[i] > max){
            max = artistID[i]
            n = i
            }
        }
    print artist[n]}
    ' million_songs_metadata_and_sales.csv
}

# Qustion 9
# Write a SQL query that returns the trackid of the song with the maximum duration
answer_9() {
    # Write a SQL query to get the answer to Q9. Do not just echo the answer.
    # Please put your SQL statement within the double quotation marks, and 
    # don't modify the command outside the double quotation marks.
    # If you need to use quotation marks in you SQL statment, please use
    # single quotation marks instead of double.
    mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT track_id 
    FROM songs 
    WHERE duration = (SELECT MAX(duration) FROM songs);"

    # # execute sql file
    # source create_tables.sql

    # LOAD DATA LOCAL INFILE '/home/ubuntu/Project3_1/million_songs_metadata.csv'
    # INTO TABLE songs 
    # FIELDS TERMINATED BY ',' 
    # LINES TERMINATED BY '\n'
    # IGNORE 0 ROWS;

    # LOAD DATA LOCAL INFILE '/home/ubuntu/Project3_1/million_songs_sales_data.csv'
    # INTO TABLE sales 
    # FIELDS TERMINATED BY ',' 
    # LINES TERMINATED BY '\n'
    # IGNORE 0 ROWS;
}

# Question 10
# A database index is a data structure that improves the speed of data retreival.
# Identify the field that will improve the performance of query in question 9 
# and create a database index on that field
INDEX_NAME="duration_index"
answer_10() {
    # Write a SQL query that will create a index on the field
    mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    CREATE INDEX duration_index ON songs (duration);"
}

# Question 11
# Write a SQL query that returns the trackid of the song with the maximum duration
# This is the same query as Question 9. Do you see any difference in performance?
answer_11() {
    # Write a SQL query to get the answer to Q11. Do not just echo the answer.
    # Please put your SQL statement within the double quotation marks, and 
    # don't modify the command outside the double quotation marks.
    # If you need to use quotation marks in you SQL statment, please use
    # single quotation marks instead of double.
    mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT track_id 
    FROM songs 
    WHERE duration = (SELECT MAX(duration) FROM songs);"
}

#Question 12
# Write the SQL query that returns all matches (across any column), 
# similar to the command grep -P 'The Beatles' | wc -l:
answer_12() {
	# Write a SQL query to get the answer to Q12. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
	SELECT COUNT(track_id) FROM songs WHERE 
    BINARY songs.title LIKE '%The Beatles%' OR 
    BINARY songs.release LIKE '%The Beatles%' OR 
    BINARY songs.artist_name LIKE '%The Beatles%';"
    #250
}

#Question 13
# Write the SQL query that returns all matches (across any column), 
# similar to the command grep -i -P 'The Beatles' | wc -l:
answer_13() {
	# Write a SQL query to get the answer to Q13. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT COUNT(track_id) FROM songs WHERE 
    title LIKE '%The Beatles%' OR 
    songs.release LIKE '%The Beatles%' OR 
    songs.artist_name LIKE '%The Beatles%';"
    #276
}

#Question 14
# Which year has the third-most number of rows in the Table songs? 
# The output should be a number representing the year
# Ignore the songs that do not have a specified year
answer_14() {
	# Write a SQL query to get the answer to Q14. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT year 
    FROM songs
    GROUP BY year
    ORDER BY COUNT(track_id) DESC
    LIMIT 3,1;"
    #2005
}

#Question 15
# Which artist has the third-most number of rows in Table songs?
# The output should be the name of the artist.
# Please use artist_id as the unique identifier of the artist
answer_15() {
	# Write a SQL query to get the answer to Q15. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT artist_name 
    FROM songs
    GROUP BY artist_id
    ORDER BY COUNT(track_id) DESC
    LIMIT 2,1;"
    #Johnny Cash
}

#Question 16
# What is the total sales count of Michael Jackson's songs?
# Your query should return a single number.
# Please use artist_id(ARXPPEY1187FB51DF4) as the unique identifier of the artist
answer_16() {
	# Write a SQL query to get the answer to Q16. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT SUM(sales_count) AS total_sales
    FROM sales
    WHERE track_id IN (SELECT track_id FROM songs WHERE artist_id = 'ARXPPEY1187FB51DF4');"
    #10100
}

#Question 17
# Which artist has the third-most number of songs that have more than 50 units in sales.
# Please use artist_name as the unique identifier of the artist.
# Output the name of the artist.
answer_17() {
	# Write a SQL query to get the answer to Q17. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "
    SELECT artist_name
    FROM songs
    WHERE track_id IN (SELECT track_id FROM sales WHERE sales_count > 50)
    GROUP BY artist_name
    ORDER BY COUNT(track_id) DESC
    LIMIT 2,1;"
    #Kenny Rogers
}

#Question 18
# Consider the following scenarios. In which of them will you choose files over databases for storing data?
# I.The application has a very high update frequency
# II.The data is stored for logging purposes (i.e. all writes are appends). 
#    It will be rarely used, except in the case of a failure
# III. The data will be updated by different users concurrently
# IV. The storage abstraction has to be able to store any data type

# A.I and II
# B.II and IV
# C.II and III
# D.I and III
answer_18() {
	# Echo a single capital letter corresponding to your choice.
	echo "B"

# Answer the following questions corresponding to your experiments 
# with sysbench benchmarks in Step 3: Vertical Scaling

# Answer the following questions corresponding to your experiments on t1.micro instance

# Question 19
# Please output the RPS (Request per Second) values obtained from 
# the first three iterations of FileIO sysbench executed on t1.micro 
# instance with magnetic EBS attached. 
answer_19() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "132.39"
	echo ,
	echo "128.46"
	echo ,
	echo "128.00"
}

# Question 20
# Please output the RPS (Request per Second) values obtained from
# the first three iterations of FileIO sysbench executed on t1.micro
# instance with SSD EBS attached. 
answer_20() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "555.63" 
	echo ,
	echo "527.34"
	echo ,
	echo "553.80"
}

# Answer the following questions corresponding to your experiments on m3.large instance

# Question 21
# Please output the RPS (Request per Second) values obtained from
# the first three iterations of FileIO sysbench executed on m3.large
# instance with magnetic EBS attached. 
answer_21() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "145.33"
	echo ,
	echo "277.39"
	echo ,
	echo "395.83"
}

# Question 22
# Please output the RPS (Request per Second) values obtained from
# the first three iterations of FileIO sysbench executed on m3.large
# instance with SSD EBS attached.
answer_22() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "1368.37"
	echo ,
	echo "1493.77"
	echo ,
	echo "1539.65"
}

# Question 23
# For the FileIO benchmark in m3.large, why does the RPS value vary in each run
# for both Magnetic and SSD-backed EBS volumes? Did the RPS value in t1.micro
# vary as significantly as in m3.large? Why do you think this is the case?
answer_23() {
	# Put your answer with a simple paragraph in a file called "answer_23"
	# Do not change the code below
	if [ -f answer_23 ]
	then
		echo "Answered"
	else
		echo "Not answered"
	fi
}



# DO NOT MODIFY ANYTHING BELOW THIS LINE

answer_7 &> /dev/null
echo "{"

echo -en ' '\"answer1\": \"`answer_1`\"
echo ","

echo -en ' '\"answer2\": \"`answer_2`\"
echo ","

echo -en ' '\"answer3\": \"`answer_3`\"
echo ","

echo -en ' '\"answer4\": \"`answer_4`\"
echo ","

echo -en ' '\"answer5\": \"`answer_5`\"
echo ","

echo -en ' '\"answer6\": \"`answer_6`\"
echo ","


if [ -f 'million_songs_metadata_and_sales.csv' ]
then
	echo -en ' '\"answer7\": \"'million_songs_metadata_and_sales.csv' file created\"
	echo ","
else
	echo -en ' '\"answer7\": \"'million_songs_metadata_and_sales.csv' file not created\"
	echo ","
fi

echo -en ' '\"answer8\": \"`answer_8`\"
echo ","

`mysql --skip-column-names --batch -u root -pdb15319root song_db -e "set global query_cache_size = 0" &> /dev/null`
`mysql --skip-column-names --batch -u root -pdb15319root song_db -e "drop index $INDEX_NAME on songs" > /dev/null`
START_TIME=$(date +%s.%N)
TID=`answer_9 | tail -1`
END_TIME=$(date +%s.%N)
RUN_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo -en ' '\"answer9\": \"$TID,$RUN_TIME\"
echo ","

answer_10 > /dev/null
INDEX_FIELD=`mysql --skip-column-names --batch -u root -pdb15319root song_db -e "describe songs" | grep MUL | cut -f1`
echo -en ' '\"answer10\": \"$INDEX_FIELD\"
echo ","

START_TIME=$(date +%s.%N)
TID=`answer_11 | tail -1`
END_TIME=$(date +%s.%N)
RUN_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo -en ' '\"answer11\": \"$TID,$RUN_TIME\"
echo ","

echo -en ' '\"answer12\": \"`answer_12`\"
echo ","

echo -en ' '\"answer13\": \"`answer_13`\"
echo ","

echo -en ' '\"answer14\": \"`answer_14`\"
echo ","

echo -en ' '\"answer15\": \"`answer_15`\"
echo ","

echo -en ' '\"answer16\": \"`answer_16`\"
echo ","

echo -en ' '\"answer17\": \"`answer_17`\"
echo ","

echo -en ' '\"answer18\": \"`answer_18`\"
echo ","

echo -en ' '\"answer19\": \"`answer_19`\"
echo ","

echo -en ' '\"answer20\": \"`answer_20`\"
echo ","

echo -en ' '\"answer21\": \"`answer_21`\"
echo ","

echo -en ' '\"answer22\": \"`answer_22`\"
echo ","

echo -en ' '\"answer23\": \"`answer_23`\"
echo 
echo  "}"