### Retrieve Degrees Awarded, merge with Enrollment to evaluate graduates taking certain classes

---
**This notebook answers the following:**

`The N and % of graduates from CLD over the past 3-4 years who received credit for CLD 102 and CLD 380`

There is also a Tableau workbook that generates the same result entitled: `CLD Graduates that took CLD102 or CLD 380 4-23-20.twb`  

---

#### Call needed Python Libraries


```python
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import numpy as np
import hana_keys
```

#### Connect to HANA database


```python
engine = create_engine('hana+pyhdb://{}:{}@{}:{}'.format(hana_keys.username, hana_keys.user_password, 'hana.uky.edu', '30015'))
```

---

#### The actual SQL code to mimic in Python

```sql
select degree.student_id,
degree.academic_term as "TERM_OF_GRADUATION",
enroll.class,
enroll.academic_term as "TERM_OF_CLASS",
case when class='CLD 102' then 1 end as "cld102_cnt",
case when class='CLD 380' then 1 end as "cld380_cnt"

from (select * from STUDENT.DEGREES_AWARDED_MULTIPLE
where degree_awarded_specialization = 'Community and Leadership Dev' and
academic_year in ('2016','2017','2018','2019') and 
data_source = 'Institutional' and 
degree_awarded_specialization_level = 'Major') degree

left join (
select academic_term, student_id, class 
from STUDENT.ENROLLMENT
where class in ('CLD 102', 'CLD 380')) enroll
on degree.student_id = enroll.student_id
```

#### Create the SQL SELECT statements


```python
sql_degrees = ('select student_id, academic_year, academic_term from STUDENT.DEGREES_AWARDED_MULTIPLE where academic_year in '
               '(\'2016\',\'2017\',\'2018\',\'2019\') and data_source = \'Institutional\' and '
               'degree_awarded_specialization_level = \'Major\' and degree_awarded_specialization = \'Community and Leadership Dev\' ')

sql_enrollment = ('select academic_term, student_id, class from STUDENT.ENROLLMENT '
                  'where class in (\'CLD 102\', \'CLD 380\')')

print (sql_degrees,'\n')
print (sql_enrollment)
```

    select student_id, academic_year, academic_term from STUDENT.DEGREES_AWARDED_MULTIPLE where academic_year in ('2016','2017','2018','2019') and data_source = 'Institutional' and degree_awarded_specialization_level = 'Major' and degree_awarded_specialization = 'Community and Leadership Dev'  
    
    select academic_term, student_id, class from STUDENT.ENROLLMENT where class in ('CLD 102', 'CLD 380')


#### Load the degrees dataframe with data from SQL


```python
%%time
df_degrees= pd.read_sql(sql_degrees, engine)
print ('the number of records retrieved:','{:,}'.format(len(df_degrees)),'\n')
df_degrees.head()
```

    the number of records retrieved: 154 
    
    CPU times: user 18.8 ms, sys: 5.26 ms, total: 24.1 ms
    Wall time: 5.36 s





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>student_id</th>
      <th>academic_year</th>
      <th>academic_term</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>60702989</td>
      <td>2016</td>
      <td>Fall 2015</td>
    </tr>
    <tr>
      <th>1</th>
      <td>60868181</td>
      <td>2016</td>
      <td>Spring 2016</td>
    </tr>
    <tr>
      <th>2</th>
      <td>60741400</td>
      <td>2016</td>
      <td>Fall 2015</td>
    </tr>
    <tr>
      <th>3</th>
      <td>60815594</td>
      <td>2016</td>
      <td>Fall 2015</td>
    </tr>
    <tr>
      <th>4</th>
      <td>60873160</td>
      <td>2016</td>
      <td>Spring 2016</td>
    </tr>
  </tbody>
</table>
</div>



#### Load the enrollment dataframe with data from SQL


```python
%%time
df_enrollment = pd.read_sql(sql_enrollment, engine)
print ('the number of records retrieved:','{:,}'.format(len(df_enrollment)),'\n')
df_enrollment.head()
```

    the number of records retrieved: 1,302 
    
    CPU times: user 30.7 ms, sys: 2.7 ms, total: 33.4 ms
    Wall time: 360 ms





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>academic_term</th>
      <th>student_id</th>
      <th>class</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Spring 2016</td>
      <td>61051012</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Spring 2016</td>
      <td>60874610</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Fall 2018</td>
      <td>60884157</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Fall 2019</td>
      <td>61298694</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Spring 2018</td>
      <td>61058913</td>
      <td>CLD 380</td>
    </tr>
  </tbody>
</table>
</div>



#### Merge the degrees and enrollment dataframes


```python
%%time
df = pd.merge(df_degrees, df_enrollment, 
              left_on= ['student_id'],
              right_on= ['student_id'], 
              how= 'left', suffixes=('', '_y'))

# Drop duplicated columns
#df.drop(list(df.filter(regex='_y$')), axis=1, inplace=True)
df.columns = ['student_id','academic_year','degree_awarded_academic_term','academic_term_class_taken','class']
# Display dataframe
df.reset_index()         # reset the index from 0
print ('\n\nnumber of records in the new dataframe:', '{:,}'.format(len(df)), '\n')
df.head(8)
```

    
    
    number of records in the new dataframe: 162 
    
    CPU times: user 9.79 ms, sys: 2.66 ms, total: 12.5 ms
    Wall time: 12.5 ms





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>student_id</th>
      <th>academic_year</th>
      <th>degree_awarded_academic_term</th>
      <th>academic_term_class_taken</th>
      <th>class</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>60702989</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Fall 2014</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>1</th>
      <td>60868181</td>
      <td>2016</td>
      <td>Spring 2016</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>60741400</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Spring 2013</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>3</th>
      <td>60815594</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>60873160</td>
      <td>2016</td>
      <td>Spring 2016</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>5</th>
      <td>60984155</td>
      <td>2016</td>
      <td>Spring 2016</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>6</th>
      <td>60764499</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>7</th>
      <td>60810115</td>
      <td>2016</td>
      <td>Spring 2016</td>
      <td>Fall 2013</td>
      <td>CLD 380</td>
    </tr>
  </tbody>
</table>
</div>



---

#### Sort dataframe


```python
df = df.sort_values(['degree_awarded_academic_term','student_id','academic_term_class_taken','class'])
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>student_id</th>
      <th>academic_year</th>
      <th>degree_awarded_academic_term</th>
      <th>academic_term_class_taken</th>
      <th>class</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>60702989</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Fall 2014</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>126</th>
      <td>60741259</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>60741400</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Spring 2013</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>125</th>
      <td>60762604</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>6</th>
      <td>60764499</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>128</th>
      <td>60791116</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>60815594</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>72</th>
      <td>60820996</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Fall 2013</td>
      <td>CLD 380</td>
    </tr>
    <tr>
      <th>73</th>
      <td>60820996</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Spring 2013</td>
      <td>CLD 102</td>
    </tr>
    <tr>
      <th>152</th>
      <td>60841728</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



#### Create counting variables


```python
df['count_cld102'] = np.where((df['class'] =='CLD 102'),1,0)
df['count_cld380'] = np.where((df['class'] =='CLD 380'),1,0)
df['count_of_graduates'] = 1
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>student_id</th>
      <th>academic_year</th>
      <th>degree_awarded_academic_term</th>
      <th>academic_term_class_taken</th>
      <th>class</th>
      <th>count_cld102</th>
      <th>count_cld380</th>
      <th>count_of_graduates</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>60702989</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Fall 2014</td>
      <td>CLD 380</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>126</th>
      <td>60741259</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>60741400</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>Spring 2013</td>
      <td>CLD 380</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>125</th>
      <td>60762604</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>60764499</td>
      <td>2016</td>
      <td>Fall 2015</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
print ('number of unique graduates:',df.student_id.nunique())
```

    number of unique graduates: 154


#### Group the dataframe by academic year and student ID


```python
df=df.groupby(['academic_year','student_id']).agg({'count_cld102':'sum','count_cld380':'sum'})
print('Records in dataframe:',len(df))
df = df.reset_index()
df.head()
```

    Records in dataframe: 154





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>academic_year</th>
      <th>student_id</th>
      <th>count_cld102</th>
      <th>count_cld380</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2016</td>
      <td>60586499</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2016</td>
      <td>60639248</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2016</td>
      <td>60639740</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2016</td>
      <td>60702989</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2016</td>
      <td>60720757</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



#### Group the dataframe by academic year


```python
df=df.groupby(['academic_year']).agg({'student_id':'count','count_cld102':'sum','count_cld380':'sum'})
df.rename(columns={'student_id':'count_of_graduates'},inplace=True)

df['% CLD102']=round(df['count_cld102']/df['count_of_graduates']*100,1)
df['% CLD380']=round(df['count_cld380']/df['count_of_graduates']*100,1)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>academic_year</th>
      <th>count_of_graduates</th>
      <th>count_cld102</th>
      <th>count_cld380</th>
      <th>% CLD102</th>
      <th>% CLD380</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2016</th>
      <td>44</td>
      <td>7</td>
      <td>11</td>
      <td>15.9</td>
      <td>25.0</td>
    </tr>
    <tr>
      <th>2017</th>
      <td>35</td>
      <td>9</td>
      <td>9</td>
      <td>25.7</td>
      <td>25.7</td>
    </tr>
    <tr>
      <th>2018</th>
      <td>33</td>
      <td>8</td>
      <td>5</td>
      <td>24.2</td>
      <td>15.2</td>
    </tr>
    <tr>
      <th>2019</th>
      <td>42</td>
      <td>7</td>
      <td>7</td>
      <td>16.7</td>
      <td>16.7</td>
    </tr>
  </tbody>
</table>
</div>



This notebook has identical output found in Tableau workbook entitled: `CLD Graduates that took CLD102 or CLD 380 4-23-20.twb`
