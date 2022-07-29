CREATE EXTERNAL TABLE mesh(term STRING,
                           tree STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES 
    ('separatorChar'=',',
    'quoteChar'='"',
    'escapeChar'='\n') 
STORED AS TEXTFILE 
LOCATION 's3://bdtt-balogun/mesh_data/';


CREATE EXTERNAL TABLE clinicaltrial_2021
    (id STRING,
    sponsor STRING,
    status STRING, 
    START STRING,
    COMPLETION STRING,
    TYPE STRING,
    submission STRING,
    conditions STRING,
    interventions STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
    'separatorChar' = '|',
    'quoteChar' = '"',
    'escapeChar' = '\n') 
STORED AS TEXTFILE 
LOCATION "s3://bdtt-balogun/clinicaltrial_data/"

CREATE EXTERNAL TABLE pharma(
    Company STRING,
    Parent_Company STRING,
    Penalty_Amount STRING,
    Subtraction_From_Penalty STRING,
    Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
    Penalty_Year INT, 
    Penalty_Date DATE, 
    Offense_Group STRING,
    Primary_Offense STRING,
    Secondary_Offense STRING,
    `Description` STRING,
    Level_of_Government STRING,
    Action_Type STRING,
    Agency STRING,
    `Civil/Criminal` STRING,
    Prosecution_Agreement STRING,
    Court STRING,
    Case_ID STRING,
    Private_Litigation_Case_Title STRING,
    Lawsuit_Resolution STRING,
    Facility_State STRING,
    City STRING,
    `Address` STRING,
    Zip STRING,
    NAICS_Code STRING,
    NAICS_Translation STRING,
    HQ_Country_of_Parent STRING,
    HQ_State_of_Parent STRING,
    Ownership_Structure STRING,
    Parent_Company_Stock_Ticker STRING,
    Major_Industry_of_Parent STRING,
    Specific_Industry_of_Parent STRING,
    Info_Source STRING,
    Notes STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES(
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\n') 
STORED AS TEXTFILE 
LOCATION 's3://bdtt-balogun/pharma_data/';

-- Question 1

SELECT COUNT(DISTINCT Id) Distinct_Rows
FROM clinicaltrial_2021
WHERE Id != 'Id' 

-- Question 2
  SELECT TYPE,
         COUNT(TYPE) Number_of_occurence
  FROM clinicaltrial_2021
GROUP BY TYPE
HAVING TYPE != 'Type'
ORDER BY Number_of_occurence DESC;

-- Question 3
SELECT diseases_conditions Individual_Conditions,
       COUNT(diseases_conditions) Frequency
FROM
  (SELECT exploded.diseases_conditions
   FROM clinicaltrial_2021
   CROSS JOIN UNNEST(SPLIT(Conditions, ',')) AS exploded(diseases_conditions)) disease_conditions_exploded
GROUP BY diseases_conditions
HAVING diseases_conditions != ''
ORDER BY Frequency DESC
LIMIT 5;

-- Question 4
SELECT SPLIT_PART(tree, '.', 1) Root,
       COUNT(tree) CountRoot
FROM
  (SELECT exploded.diseases_conditions
   FROM clinicaltrial_2021
   CROSS JOIN UNNEST(SPLIT(Conditions, ',')) AS exploded(diseases_conditions)) disease_conditions_exploded
INNER JOIN mesh ON diseases_conditions = mesh.term
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;

-- Question 5
SELECT Sponsor,
       COUNT(Sponsor) NumberOfStudies
FROM clinicaltrial_2021
WHERE Sponsor NOT IN
    (SELECT DISTINCT Parent_Company
     FROM pharma)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- Question 6
WITH get_months AS
  (SELECT SPLIT_PART(COMPLETION, ' ', 1) MonthsOfYear
   FROM clinicaltrial_2021
   WHERE COMPLETION LIKE '%2021'
     AND status = 'Completed'),
     MonthsOrder AS
  (SELECT MonthsOfYear,
          COUNT(MonthsOfYear) Freq,
          CASE
              WHEN MonthsOfYear = 'Jan' THEN 1
              WHEN MonthsOfYear = 'Feb' THEN 2
              WHEN MonthsOfYear = 'Mar' THEN 3
              WHEN MonthsOfYear = 'Apr' THEN 4
              WHEN MonthsOfYear = 'May' THEN 5
              WHEN MonthsOfYear = 'Jun' THEN 6
              WHEN MonthsOfYear = 'Jul' THEN 7
              WHEN MonthsOfYear = 'Aug' THEN 8
              WHEN MonthsOfYear = 'Sep' THEN 9
              WHEN MonthsOfYear = 'Oct' THEN 10
              WHEN MonthsOfYear = 'Nov' THEN 11
              WHEN MonthsOfYear = 'Dec' THEN 12
          END OrderOfMonths
   FROM get_months
   GROUP BY 1
   ORDER BY 3)
SELECT MonthsOfYear,
       Freq
FROM MonthsOrder ;