import pandas as pd
import streamlit as st
import mysql.connector

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database="2582"
)

# Create a cursor
mysql_cursor = mysql_db.cursor()

# queries
queries = {

    "1. What are the names of all the channels?": "SELECT Channel_Name FROM channel",
    "2. What are the names of all the videos and their corresponding channels?": "SELECT v.Video_Name, c.Channel_Name FROM video v INNER JOIN channel c ON v.Channel_id = c.Channel_id",
    "3. Which channels have the most number of videos, and how many videos do they have?": "SELECT c.Channel_Name, COUNT(v.Video_id) AS Video_Count FROM channel c INNER JOIN video v ON c.Channel_id = v.Channel_id GROUP BY c.Channel_Name ORDER BY Video_Count DESC LIMIT 10",
    "4. What are the top 10 most viewed videos and their respective channels?": "SELECT v.Video_Name, c.Channel_Name FROM video v INNER JOIN channel c ON v.Channel_id = c.Channel_id ORDER BY v.View_Count DESC LIMIT 10",
    "5. How many comments were made on each video, and what are their corresponding video names?": "SELECT v.Video_Name, SUM(v.Comment_Count) AS Total_Comment_Count FROM video v GROUP BY v.Video_Name",
    "6. Which videos have the highest number of likes, and what are their corresponding channel names?": "SELECT v.Video_Name, c.Channel_Name FROM video v INNER JOIN channel c ON c.Channel_id = c.Channel_id ORDER BY v.Like_Count DESC LIMIT 10",
    "7. What is the total number of likes and dislikes for each video, and what are their corresponding video names?": "SELECT v.Video_Name, SUM(v.Like_Count) AS Total_Likes, SUM(v.Dislike_Count) AS Total_Dislikes FROM video v GROUP BY v.Video_Name",

    "8. What is the total number of views for each channel, and what are their corresponding channel names?": "SELECT c.Channel_Name, SUM(v.View_Count) AS Total_Views FROM channel c INNER JOIN video v ON c.Channel_id = v.Channel_id GROUP BY c.Channel_Name",
    
    "9. What are the names of all the channels that have published videos in the year 2022?": "SELECT DISTINCT c.Channel_Name FROM channel c INNER JOIN video v ON c.Channel_id = c.Channel_id WHERE YEAR(v.PublishedAt) = 2022",
    "10. What is the average duration of all videos in each channel, and what are their corresponding channel names?": "SELECT c.Channel_Name, SEC_TO_TIME(AVG(TIME_TO_SEC(v.Duration))) AS Average_Duration FROM channel c INNER JOIN video v ON c.Channel_id = v.Channel_id WHERE v.Duration IS NOT NULL AND v.Duration <> ''GROUP BY c.Channel_Name",
    "11. Which videos have the highest number of comments, and what are their corresponding channel names?": "SELECT v.Video_Name, c.Channel_Name FROM video v INNER JOIN channel c ON v.Channel_id = c.Channel_id ORDER BY v.Comment_Count DESC LIMIT 10"
    
}

# Display query selection dropdown
query_selection = st.selectbox("Select a query", list(queries.keys()))

if query_selection == "1. What are the names of all the channels?":
    try:
        # Execute the query to retrieve channel names
        query = queries[query_selection]
        mysql_cursor.execute(query)
        result_data = mysql_cursor.fetchall()

        # Create a DataFrame from the query result
        df = pd.DataFrame(result_data, columns=["Channel Name"])

        # Display the DataFrame in Streamlit as a table
        st.header(query_selection)
        st.table(df)

    except mysql.connector.Error as err:
        st.error(f"Error executing query: {err}")

else:
    # Execute the selected query
    query = queries[query_selection]

    try:
        mysql_cursor.execute(query)
        result_data = mysql_cursor.fetchall()

        # Create a DataFrame from the query result
        df = pd.DataFrame(result_data, columns=[desc[0] for desc in mysql_cursor.description])

        # Display the DataFrame in Streamlit
        st.header(query_selection)
        st.dataframe(df)

    except mysql.connector.Error as err:
        st.error(f"Error executing query: {err}")