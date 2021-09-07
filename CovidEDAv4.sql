-- Database: PortfolioProject

-- DROP DATABASE "PortfolioProject";

CREATE DATABASE "PortfolioProject"
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_Guyana.1252'
    LC_CTYPE = 'English_Guyana.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
	

CREATE TABLE CD (
iso_code varchar,
continent varchar,
location_ varchar,	
date_ date,	
population bigint ,	
total_cases int,	
new_cases int,	
new_cases_smoothed decimal,	
total_deaths int,
new_deaths int,	
new_deaths_smoothed decimal,
total_cases_per_million	decimal,
new_cases_per_million decimal,	
new_cases_smoothed_per_million decimal,
total_deaths_per_million decimal,	
new_deaths_per_million decimal,	
new_deaths_smoothed_per_million decimal,
reproduction_rate decimal,	
icu_patients decimal,	
icu_patients_per_million decimal,	
hosp_patients int,	
hosp_patients_per_million decimal,	
weekly_icu_admissions decimal,	
weekly_icu_admissions_per_million decimal,	
weekly_hosp_admissions decimal,	
weekly_hosp_admissions_per_million decimal
);




copy CD
FROM 'C:/Users/dlamb/Downloads/CovidDeaths.csv'
DELIMITER ','
CSV HEADER;

select * from cd


CREATE TABLE CV (

iso_code varchar,	
continent varchar,	
location_ varchar,	
date_ date,	
new_tests int,	
total_tests int,	
total_tests_per_thousand decimal,	
new_tests_per_thousand	decimal,
new_tests_smoothed decimal,	
new_tests_smoothed_per_thousand	decimal,
positive_rate  decimal,	
tests_per_case decimal,	
tests_units varchar,	
total_vaccinations decimal,
people_vaccinated varchar,	
people_fully_vaccinated	bigint,
total_boosters bigint,
new_vaccinations bigint,	
new_vaccinations_smoothed decimal,	
total_vaccinations_per_hundred decimal,	
people_vaccinated_per_hundred decimal,	
people_fully_vaccinated_per_hundred decimal,	
total_boosters_per_hundred decimal,	
new_vaccinations_smoothed_per_million decimal,	
stringency_index decimal,	
population_density decimal,	
median_age decimal,	
aged_65_older decimal,
aged_70_older decimal,	
gdp_per_capita decimal,
extreme_poverty	decimal,
cardiovasc_death_rate decimal,
diabetes_prevalence decimal,	
female_smokers decimal,	
male_smokers decimal,	
handwashing_facilities decimal,	
hospital_beds_per_thousand decimal,	
life_expectancy	decimal,
human_development_index	decimal,
excess_mortality decimal

);

copy CV
FROM 'C:/Users/dlamb/Downloads/CovidVacs.csv'
DELIMITER ','
CSV HEADER;

SELECT location_, date_, total_cases, new_cases, total_deaths, population
FROM CD
order by location_ , date_

				/* Let's examine total cases vs total deaths*/

alter table cd alter column total_deaths set data type decimal(15,2)
alter table cd alter column total_cases set data type decimal(15,2)

SELECT location_, date_, total_deaths,total_cases,
CAST(total_deaths/total_cases AS decimal(10,5))*100 as Death_Rate
FROM CD
WHERE location_ like 'Guyana'
order by location_ , date_

				/* Between March 12 2020 and September 5 2021 there's roughly a 2.5% chance of death due to covid */

				/*Examining Total Cases vs Population for Guyana*/
				
SELECT location_, date_,total_cases, population,
CAST(total_cases/population AS decimal(15,5))*100 as Infection_Rate
FROM CD
WHERE location_ like 'Guyana'
order by location_ , date_

				/* Infections were considerably low in Guyana until early 2021 when 3% of the pop was infected*/

/*Let's find the highest rate of infection*/

SELECT location_,max(total_cases) as HighestInfections, population,
CAST(max(total_cases/population) AS decimal(15,5))*100 as Infection_Rate
FROM CD
WHERE location_ like 'Guyana'
GROUP BY location_ , population
order by location_ , population

/*Let's find the highest rate of infection Globally*/

SELECT location_,max(total_cases) as HighestInfectionsSum, population,
CAST(max(total_cases/population) AS decimal(15,5))*100 as Infection_Rate_Percentage
FROM CD
/*WHERE location_ like '%States%'*/
GROUP BY location_ , population
order by Infection_Rate_Percentage desc
/* Quite a few small islands had zero infections, that's amazing*/

/* Let's look at the highest amount of deaths per country*/

SELECT location_, max(total_deaths) AS TotalDeathCount
FROM CD
where total_deaths is not null
GROUP BY location_
ORDER BY TotalDeathCount desc

/*Globally, 4mil deaths, Europe with 1.2 mil in second place*/

/* Exploring by Continent*/

Select continent, MAX(total_deaths) as TotalDeathCount
From CD
Where continent is not null 
Group by continent
order by TotalDeathCount desc

/*Further Global Numbers using aggregation*/

alter table cd alter column new_deaths set data type decimal(15,2)
alter table cd alter column new_cases set data type decimal(15,2)

SELECT date_, SUM(new_cases) AS total_cases, 
SUM(new_deaths) as total_deaths, 
SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
FROM CD
WHERE continent is not null
GROUP BY date_
ORDER BY total_cases, total_deaths , date_

/* In total */

SELECT SUM(new_cases) AS total_cases, 
SUM(new_deaths) as total_deaths, 
SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
FROM CD
WHERE continent is not null
/*GROUP BY date_*/
ORDER BY total_cases, total_deaths 

/* This means just over 2% of the world's population has died of covid*/
/*Let's examine how vaccinations has affected these numbers globally*/

SELECT CD.continent, CD.location_, CD.date_, CD.population, CV.new_vaccinations,
SUM (cast(CV.new_vaccinations as int)) 
OVER (PARTITION by CV.location_ order by CD.location_, CD.date_) as RollingCount /*so that the agg is location specific*/
FROM CD
JOIN CV 
	ON CD.location_ = CV.location_
	AND CD.date_ = CV.date_
/*WHERE CD.continent is not null,*/
WHERE CD.location_ like 'Guyana'
ORDER BY 2,3

/*According to the summary, there are 29,818 vaccinated Guyanese as of Sep, 2021*/
/* Now let's use a CTE to compare the rolling count of vaccinated people to the population*/




With PopvsVac (continent, location_, date_, population, new_vaccinations, RollingCount)
as
(
Select CD.continent, CD.location_, CD.date_, CD.population, CV.new_vaccinations,
SUM(CAST(CV.new_vaccinations AS int)) OVER (Partition by CD.Location_ Order by CD.location_, CD.date_) as RollingCount
From CD
Join CV
	On CD.location_ = CV.location_
	and CD.date_ = CV.date_
where CD.continent is not null 

)

Select *, (RollingCount/population)*100 as VaccinationPercent
From PopvsVac

/* Let me create some views to be accessed when visualizing this data*/

CREATE VIEW PopvsVac as 
Select CD.continent, CD.location_, CD.date_, CD.population, CV.new_vaccinations,
SUM(CAST(CV.new_vaccinations AS int)) OVER (Partition by CD.Location_ Order by CD.location_, CD.date_) as RollingCount
From CD
Join CV
	On CD.location_ = CV.location_
	and CD.date_ = CV.date_
where CD.continent is not null

CREATE VIEW DeathsvsTotalCases as
SELECT SUM(new_cases) AS total_cases, 
SUM(new_deaths) as total_deaths, 
SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
FROM CD
WHERE continent is not null
/*GROUP BY date_*/
ORDER BY total_cases, total_deaths 

CREATE VIEW DeathByContinent as
Select continent, MAX(total_deaths) as TotalDeathCount
From CD
Where continent is not null 
Group by continent
order by TotalDeathCount desc




