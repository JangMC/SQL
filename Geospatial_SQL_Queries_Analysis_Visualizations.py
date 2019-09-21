

# ### Loading the Dataset CSV file
# 
# Three datasets are need for this project:
# 1. The Chicago police stations in every district
# 2. The Boundaries.geojson data for district boundries
# 3. The Crimes dataset 
# 
# 
# Lets load the CSV file into a DataFrame object and see the nature of the data that we have.
# 
# Complete description of the dataset can be found on Chicago city data portal.
# 
# Based on Trumps State of the Uniion Address and the article written by  columnist Clarence Page and  published by the Chicago Tribune, we are interested to retrieve the data for the past two years and perform different types of spatial queries.
# 
# There are few of these queries that we are interested in to help CPD and city of Chicago to plot on a Choroplteh map those districts that have highest gun crimes. 
# 
# Here are examples of those types of quereis:
# 
# 1. Plot on **Choropleth map** the **districts** and their **Violent Crimes**
# 2. Plot on Choropleth map the districts and their **Gun** related crimes
# 3. Which district is the **crime capital** of **Chicago districts**?
# 4. What the **crime density** per **district**?
# 5. Plot on Choropleth map those **gun related crimes** that resulted in **arrests**
# 5. Plot on Choropleth map the gun related crime that is in the **farthest Block**  from the **policy stattion** for every **district**  
# 
# 
# 

# Packages you need to Connect **PostgreSQL** server to load and retrieve Crhicago Crime dataset from the database:
# 
# 1. **psycopg2:** for PostgreSQL driver
# 2. **area:** to calculate the area inside of any GeoJSON geometry
# 3. **Folium:** for Choropleth maps
# 
# 
# Since we are using PostGIS in our work, please read and bookmark __[Chapter 4. Using PostGIS: Data Management and Queries](https://postgis.net/docs/manual-1.4/ch04.html)__ 
# 

# In[1]:


import folium
from folium import plugins
from folium.plugins import MarkerCluster
import psycopg2
import csv
import pandas as pd
import json
from area import area

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 



# In[2]:



db_connection = psycopg2.connect(host='129.105.208.229',dbname="chicago_crimes", user="username" , password="password")


cursor = db_connection.cursor()


# ### Chicago Crimes Dataset
# 
# The Crimes_2001_to_present.csv is downloaded from Chicago data portal and it has roughly 6.5 million records.
# 
# While working on this dataset, It is prudent to make a note of the following:
# 1. Geospatial  queries are very demanding for system resouces like CPU, Memory, and DISK
# 1. We are interested in the data set of the past 2 years, and when you execute Geospatial type queries, please be advised that these queries slow down your machine. 
# 2. Running this script to work on the data of the past 2 years will require roughly 25 minutes to complete. And requires roughly 40 minutes to complete using the dataset of the past 5 years. And requires hours to complete on the entire dataset with at least 16GB memory.
# 3. It is a good idea to take a slice (past two years) of the dataset and store it, that will help improve perfoamnce significantly especialy for SEARCH and SORT algorithms that are utilized by the database engine.
# 
# 
# ### Algorithm Performance
# 
# - **Sort algorithms** used by the database engines vary in performance between O($N log N$) and O($ N^{2} $) where $N$ is the size of the number
# 
# - **Search algorithms** used by the database engines vary in performance between O($log N$) and O($ N $) where $N$ is the size of the number
# 
# 
# 

# ## Lets start executing different  Queries

# ## Query #1:
# - Calculate the total number of crimes in every district and plot that on Choropleth map

# In[3]:


cursor.execute("SELECT district, count(district) from crimes GROUP BY district")
rows=cursor.fetchall()


# In[4]:


crimes_per_district = pd.DataFrame(rows, columns=['dist_num','number_of_crimes'])
crimes_per_district['dist_num'] = crimes_per_district['dist_num'].astype(str)

crimes_per_district.head()


# In[5]:


total_number_of_crimes_per_district_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)


# In[6]:


total_number_of_crimes_per_district_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='OrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = crimes_per_district,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'number_of_crimes']
              )


# In[7]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("SELECT district, count(district) from crimes where district= %s GROUP BY district",[police_station[2]])
    districts_crime_numbers = cursor.fetchall()
    for district in districts_crime_numbers:
        folium.Marker(location = police_station_location,popup=folium.Popup(html="District No : %s  has   Total Number of Crimes:%s" %district ,max_width=450)).add_to(total_number_of_crimes_per_district_map)


# - **Lets plot the Choropleth map and notice  the intensity of color on the different districts**
# - **The Blue POPUP represents the location of police station in the different districts in the map**

# In[8]:



total_number_of_crimes_per_district_map


# ## Query #2:
# - Calculate the total number of **violent crimes** in every district and plot that in a table on Choropleth map

# Well, we really need only the violent crimes per district, so we will filter only those crimes that we are interested in. Please note that we are not interested to plot property crimes, we are really after violent crimes and in particular **Gun** related crimes.
# 
# So for now, lets plot violent crimes on Choropleth map and later on we will filter only Gun related crimes

# In[9]:


violent_crime_categories='THEFT','ASSAULT','ROBBERY','KIDNAPPING','CRIM SEXUAL ASSAULT','BATTERY','MURDER'


# In[10]:


cursor.execute("SELECT district, count(district) from crimes where PRIMARY_TYPE in %s GROUP BY district",[violent_crime_categories])
rows=cursor.fetchall()
violent_crime_data=pd.DataFrame(rows, columns=['district_num','number_of_violent_crimes'])
violent_crime_data['district_num'] = violent_crime_data['district_num'].astype(str)
violent_crime_data


# In[11]:


violent_crimes_per_district_map= folium.Map(location =(41.8781, -87.6298),zoom_start=11)
violent_crimes_per_district_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = violent_crime_data,
              key_on='feature.properties.dist_num',
              columns = ['district_num', 'number_of_violent_crimes'],
              legend_name="VOILENT CRIME MAP"
              )


# In[12]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location =(police_station[0],police_station[1])
    cursor.execute("SELECT PRIMARY_TYPE, count(PRIMARY_TYPE) from crimes where district =%s AND PRIMARY_TYPE in %s GROUP BY PRIMARY_TYPE",[police_station[2],violent_crime_categories])
    data = cursor.fetchall()
    violent_crimes_per_district_df = pd.DataFrame(data, columns=['Description', 'Number of Violent Crimes'])
    header = violent_crimes_per_district_df.to_html(classes='table table-striped table-hover table-condensed table-responsive')
    folium.Marker(location=police_station_location, popup=folium.Popup(html="District Number %s - Violent Crimes %s" %(police_station[2],header))).add_to(violent_crimes_per_district_map)



# In[13]:


violent_crimes_per_district_map


# ## Query #3:
# - Calculate the total number of **gun related violent crimes** in every district and plot that in a table on Choropleth map

# 
# Lets first create a dataframe of gun crimes per district first to get an idea about the number of gun crimes per district
# 

# In[14]:



gun='%GUN%'
cursor.execute("SELECT district, count(district) from crimes where DESCRIPTION::text LIKE %s GROUP BY district",[gun])
districts_gun_violent_crimes = cursor.fetchall()
districts_gun_violent_crimes_df = pd.DataFrame(districts_gun_violent_crimes, columns=['dist_num','gun_crimes'])
districts_gun_violent_crimes_df['dist_num'] = districts_gun_violent_crimes_df['dist_num'].astype(str)
districts_gun_violent_crimes_df


# In[15]:


districts_gun_violent_crimes_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
districts_gun_violent_crimes_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_gun_violent_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME"
              )


# Now, lets create a dataframe of the **different types of gun crimes for every district** and then plot it on Choropleth map

# In[16]:



cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")

gun='%GUN%'
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("""SELECT DESCRIPTION, count(DESCRIPTION) from crimes where district=%s and DESCRIPTION::text LIKE %s GROUP BY DESCRIPTION""",[police_station[2],gun])
    district_gun_violent_crimes=cursor.fetchall()
    district_gun_violent_crimes_df=pd.DataFrame(district_gun_violent_crimes, columns=['Description', 'Number of Gun Crime'])
    header = district_gun_violent_crimes_df.to_html(classes='table table-striped table-hover table-condensed table-responsive')
    folium.Marker(location=police_station_location,popup=folium.Popup(html="District No: %s GUN_Crime: %s" %(police_station[2],header) )).add_to(districts_gun_violent_crimes_map)
    


# In[17]:


districts_gun_violent_crimes_map


# ## Query #4:
# - Calculate the crime density per district

# In[18]:


district=[]
tarea=[]

with open('Boundaries.geojson') as f:
    data = json.load(f)
    a = data['features']
    for i in range(len(a)):
        obj=a[i]['geometry']
        n= a[i]['properties']
        district.append(n['dist_num'])
        tarea.append(area(obj)/10000)

af=pd.DataFrame({'dist_num': district,'district_area_inHectares':tarea})
af['dist_num'] = af['dist_num'].astype(str)
final_data= pd.merge(af, crimes_per_district, on='dist_num', how='inner')
final_data['crime_density'] = round(final_data['number_of_crimes']/(final_data['district_area_inHectares']/100))
final_data


# ## Query #5:
# - Create **Marker Clusters** on Choropleth map for those **gun related violent crimes** that resulted in **arrest (green icon)** and those that **didn't (red icon)**

# In[19]:


gun_crime_arrests_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
gun_crime_arrests_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_gun_violent_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME"
              )


# In[20]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
gun='%GUN%'

police_stations = cursor.fetchall()

marker_cluster = MarkerCluster().add_to(gun_crime_arrests_map)

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("""SELECT DISTINCT ON(caseno) caseno, block,DESCRIPTION, count(arrest), arrest,latitude, longitude from crimes where district=%s and DESCRIPTION::text LIKE %s GROUP BY caseno,block, DESCRIPTION,arrest, latitude, longitude""",[police_station[2],gun])
    crimes_per_district = cursor.fetchall()
    for crime in crimes_per_district:
        if crime[4]==True:
            folium.Marker(location=(crime[5],crime[6]),popup=folium.Popup(html="District No: %s <br> Description: %s <br> Block: %s" %(police_station[2],crime[2],crime[1])),icon=folium.Icon(color='green', icon='ok-sign'),).add_to(marker_cluster)
        else:
            folium.Marker(location=(crime[5],crime[6]),popup=folium.Popup(html="District No: %s <br> Description: %s<br> Block: %s" %(police_station[2],crime[2],crime[1])),icon=folium.Icon(color='red', icon='remove-sign'),).add_to(marker_cluster)

            


# In[21]:


gun_crime_arrests_map


# ## Query #6:
# - Plot on Choropleth map the **farthest Block** that has a gun crime from every police station in every district 

# In[22]:


farthest_block_gun_crime_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
farthest_block_gun_crime_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_gun_violent_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME"
              )


# In[23]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
police_stations = cursor.fetchall()

gun='%GUN%'

for police_station in police_stations:
    
    cursor.execute("""SELECT DISTINCT on (A.block) A.district,A.block, A.where_is,ST_Distance(A.where_is,B.where_is) from crimes as A, police_stations as B 
    where ST_Distance(A.where_is,B.where_is) in ( SELECT max(dist) FROM 
    (SELECT ST_Distance(A.where_is,B.where_is) as dist from crimes as A, police_stations as B where A.district=%s 
    and DESCRIPTION::text LIKE %s and B.district= %s ) as f)""",[police_station[2],gun,police_station[2]])
    
    farthest_block_gun_crime = cursor.fetchall()

    cursor.execute("SELECT ST_X(ST_AsText(%s)), ST_Y(ST_AsText(%s))",(farthest_block_gun_crime[0][2],farthest_block_gun_crime[0][2]))
    farthest_block_gun_crime_location = cursor.fetchall()
    folium.Marker(location=(police_station[0],police_station[1]),popup=folium.Popup(html="Police Station <br> District No.:%s <br> Farthest Gun_Crime Block:%s"%(farthest_block_gun_crime[0][0],farthest_block_gun_crime[0][1]))).add_to(farthest_block_gun_crime_map)
    folium.CircleMarker(farthest_block_gun_crime_location[0],radius=5,color='#ff3187',popup=folium.Popup(html="District No.:%s <br> Block:%s"%(farthest_block_gun_crime[0][0],farthest_block_gun_crime[0][1]))).add_to(farthest_block_gun_crime_map) 


# In[24]:


farthest_block_gun_crime_map


# # Requirements
# 
# 
# 
# ** The PDF document your are submitting must have the source code and the output for the following  requirements **
# 

# ### Requirement #1: 
# - Locate the **Block** that has the **higest number of gun crimes**. The popup on Choropleth map shall display the Block in every district along with the total number of gun crimes for that block

# In[25]:


blocks_gun_crime_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
blocks_gun_crime_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_gun_violent_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME"
              )


# In[26]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")

gun='%GUN%'
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("""SELECT BLOCK, count(BLOCK) from crimes where district=%s and DESCRIPTION::text LIKE %s GROUP BY BLOCK ORDER BY COUNT(BLOCK) DESC LIMIT 1""",[police_station[2],gun])
    
    block_gun_violent_crimes=cursor.fetchall()
    
    block_gun_violent_crimes_df=pd.DataFrame(block_gun_violent_crimes, columns=['Block', 'Number of Gun Crime'])
    header = block_gun_violent_crimes_df.to_html(classes='table table-striped table-hover table-condensed table-responsive')
    folium.Marker(location=police_station_location,popup=folium.Popup(html="District No: %s Block with Highest Gun Crimes: %s" %(police_station[2],header) )).add_to(blocks_gun_crime_map)
    


# In[27]:


blocks_gun_crime_map


# ### Requirement #2: 
# - Calculate the gun crimes density in every district

# In[28]:


district=[]
tarea=[]

with open('Boundaries.geojson') as f:
    data = json.load(f)
    a = data['features']
    for i in range(len(a)):
        obj=a[i]['geometry']
        n= a[i]['properties']
        district.append(n['dist_num'])
        tarea.append(area(obj)/10000)

ab=pd.DataFrame( {'dist_num': district, 'district_area_inHectares' :tarea})
ab['dist_num'] = ab['dist_num'].astype(str)

finaldata = pd.merge (ab, districts_gun_violent_crimes_df, on= 'dist_num', how= 'inner')
finaldata['gun_crime_density'] = round (finaldata['gun_crimes'] / (finaldata ['district_area_inHectares']/100))



# In[29]:


finaldata


# ### Requirement #3: 
# - Locate the **farthest** UNLAWFUL POSS OF HANDGUN crime from the police station in every district. The popup on Choropleth map shall display the district number and the block

# In[30]:


cursor.execute("SELECT district, count(district) from crimes where DESCRIPTION = 'UNLAWFUL POSS OF HANDGUN' GROUP BY district")
districts_upohg_crimes = cursor.fetchall()
districts_upohg_crimes_df = pd.DataFrame(districts_upohg_crimes, columns=['dist_num','unlaw_pos_handgun_crimes'])
districts_upohg_crimes_df ['dist_num'] = districts_upohg_crimes_df ['dist_num'].astype(str)
districts_upohg_crimes_df


# In[31]:


farthest_upohg_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
farthest_upohg_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_upohg_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'unlaw_pos_handgun_crimes'],
              legend_name="UNLAWFUL POSESSION OF HANDGUN CRIME"
              ) 


# In[32]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
police_stations = cursor.fetchall()

upohgc='UNLAWFUL POSS OF HANDGUN'



for police_station in police_stations:
    
    cursor.execute("""SELECT DISTINCT on (A.block) A.district,A.block, A.where_is,ST_Distance(A.where_is,B.where_is) from crimes as A, police_stations as B 
    where ST_Distance(A.where_is,B.where_is) in ( SELECT max(dist) FROM 
    (SELECT ST_Distance(A.where_is,B.where_is) as dist from crimes as A, police_stations as B where A.district=%s 
    and DESCRIPTION::text LIKE %s and B.district= %s ) as f)""",[police_station[2],upohgc,police_station[2]])
    
    farthest_upohg_crime = cursor.fetchall()

    cursor.execute("SELECT ST_X(ST_AsText(%s)), ST_Y(ST_AsText(%s))",(farthest_upohg_crime[0][2],farthest_upohg_crime[0][2]))
    
    farthest_upohg_crime_location = cursor.fetchall()
    
    folium.Marker(location=(police_station[0],police_station[1]),popup=folium.Popup(html="Police Station <br> District No.:%s <br> Farthest Unlawful Poss. of Handgun:%s"%(farthest_upohg_crime[0][0],farthest_upohg_crime[0][1]))).add_to(farthest_upohg_map)
    folium.CircleMarker(farthest_upohg_crime_location[0],radius=5,color='#ff3187',popup=folium.Popup(html="District No.:%s <br> Block:%s"%(farthest_upohg_crime[0][0],farthest_upohg_crime[0][1]))).add_to(farthest_upohg_map) 
  


# In[33]:


farthest_upohg_map


# ### Requirement #4: 
# 
# - Create **Marker Clusters** on Choropleth map for those **gun related violent crimes** that have Location Desciption as RESIDENCE in ** (green icon)** and those that have Location Desciption as STREET in **(red icon)**

# In[34]:


gun_crime_location_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
gun_crime_location_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_gun_violent_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME"
              )


# In[35]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
gun='%GUN%'

police_stations = cursor.fetchall()

marker_cluster = MarkerCluster().add_to(gun_crime_location_map)

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("""SELECT DISTINCT ON(caseno) caseno, block, description, count (description), location_description, latitude, longitude from crimes where district=%s and DESCRIPTION::text LIKE %s and (location_description = 'RESIDENCE' or location_description = 'STREET') GROUP BY caseno,block, DESCRIPTION, location_description, latitude, longitude""",[police_station[2],gun])
    crimes_per_area = cursor.fetchall()
    for crime in crimes_per_area:
        if crime[4]=='RESIDENCE':
            folium.Marker(location=(crime[5],crime[6]),popup=folium.Popup(html="District No: %s <br> Description: %s <br> Block: %s" %(police_station[2],crime[2],crime[1])),icon=folium.Icon(color='green', icon='ok-sign'),).add_to(marker_cluster)
        else:
            folium.Marker(location=(crime[5],crime[6]),popup=folium.Popup(html="District No: %s <br> Description: %s<br> Block: %s" %(police_station[2],crime[2],crime[1])),icon=folium.Icon(color='red', icon='ok-sign'),).add_to(marker_cluster)

            



# In[36]:


gun_crime_location_map


# In[ ]:




