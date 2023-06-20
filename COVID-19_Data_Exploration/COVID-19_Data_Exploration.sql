-- Looking the entire dataset of COVID-19 Deaths Record
SELECT*
FROM SQLDataExploration..CovidRecords
ORDER BY 3, 4


-- Indexing location
CREATE INDEX loc_id
ON SQLDataExploration..CovidRecords (location)


-----------------------------------------------Infection---------------------------------------------------

-- COVID-19 infection rate in Indonesia 
SELECT location, date, population, total_cases, (total_cases/population)*100 as InfectionRate
FROM SQLDataExploration..CovidRecords
WHERE location = 'Indonesia'
ORDER BY 1,2


-- Ranking locations by the highest proportion of population infected by COVID-19
SELECT location, population, MAX(CONVERT(float, total_cases)) as HighestInfection, MAX((CONVERT(float, total_cases)/population))*100 as InfectionRate
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY location, population
ORDER BY InfectionRate desc


-- Top 10 Countries with the highest total cases
SELECT TOP (10) location, MAX(CONVERT(float,total_cases))/1000000 as TotalCasesPerMillion
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY location
ORDER BY 2 desc
-- in this case, using total_cases_per_million does not work well


-----------------------------------------------Deaths---------------------------------------------------

-- Cases and total deaths record 
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM SQLDataExploration..CovidRecords
ORDER BY 1,2


-- Average deaths of each continent in the last 7 days
SELECT continent, ROUND(SUM(CONVERT(float, new_deaths))/7,1) as AverageDeathsPerDay
FROM SQLDataExploration..CovidRecords
WHERE continent is not null 
	AND date BETWEEN '2023-05-07' AND '2023-05-13'
	--AND date >= (SELECT DATEADD(day, -7, MAX(date)) FROM SQLDataExploration..CovidRecords)
GROUP BY continent
ORDER BY AverageDeathsPerDay desc


-- Death probability by COVID-19 in Indonesia
SELECT location, date, total_cases, total_deaths, (CONVERT(float, total_deaths)/CONVERT(float, total_cases))*100 as DeathPercentage
FROM SQLDataExploration..CovidRecords
WHERE location = 'Indonesia'
ORDER BY 1,2


-- Stringency index correlation with total deaths of each country
SELECT location, ROUND(AVG(stringency_index),2) as AverageStringencyIndex, ROUND(MAX(CONVERT(float, total_deaths))/population,5) as DeathRatio
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY location, population
ORDER BY AverageStringencyIndex desc


-- Ranking locations by the highest COVID-19 mortality 
SELECT location, population, MAX(CONVERT(float, total_deaths)) as HighestDeath
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY location, population
ORDER BY HighestDeath desc


-- Total of COVID-19 deaths for each continent
SELECT continent, SUM(CONVERT(int, new_deaths)) as HighestDeath
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY continent
ORDER BY HighestDeath desc


-- Total of COVID-19 deaths for each country
SELECT location, MAX(CONVERT(float, total_deaths)) as HighestDeath
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY location
ORDER BY HighestDeath desc


-- Global records for new cases, new deaths, with death percentage each day
SELECT date, SUM(new_cases) as new_cases_global, SUM(cast(new_deaths as int)) as new_deaths_global, (SUM(cast(new_deaths as int))/SUM(new_cases))*100 as DeathPercentage
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY date
ORDER BY 1,2


-- Trigger alerts if new_deaths is over 1000 in updated row

USE SQLDataExploration

CREATE TABLE alerts (
  location VARCHAR(255),
  date DATETIME,
  new_deaths INT
)

CREATE TRIGGER DeathsAlert 
ON SQLDataExploration.dbo.CovidRecords
AFTER INSERT AS 
BEGIN
	INSERT INTO alerts(location, date, new_deaths)
	SELECT location, date, new_deaths
	FROM inserted
	WHERE new_deaths > 1000
END

INSERT INTO SQLDataExploration.dbo.CovidRecords (location, date, new_deaths)
VALUES ('United States', '2023-05-18', 1500)

SELECT * FROM alerts

DELETE FROM CovidRecords
WHERE location = 'United States' AND date = '2023-05-18'

DELETE FROM alerts
WHERE location = 'United States' AND date = '2023-05-18'


-- Mortality rate of the given country
CREATE FUNCTION ExcessMortalityRate (@country VARCHAR(255))
RETURNS FLOAT
AS
BEGIN
	DECLARE @excess_mortality FLOAT
	SELECT @excess_mortality = excess_mortality
	FROM SQLDataExploration.dbo.CovidRecords
	WHERE location = @country
	RETURN @excess_mortality
END

SELECT dbo.ExcessMortalityRate('United States');

SELECT * FROM sys.objects WHERE type = 'FN'

-----------------------------------------------Vaccination---------------------------------------------------


-- COVID-19 vaccination progress by location and date
SELECT continent, location, date, population, new_people_vaccinated_smoothed, SUM(CONVERT(float, new_people_vaccinated_smoothed)) OVER (Partition by location
ORDER by date) as NumberPeopleVaccinated
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
ORDER BY 1,2,3


-- The impact of vaccination on deaths in different continents and locations
SELECT continent, location, date, population, new_people_vaccinated_smoothed, CONVERT(int, new_deaths) as deaths_records
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
ORDER BY 1,2,3


-- Top 10 countries with the highest fully vaccinated rate by 13 May 2023
SELECT TOP (10) location, MAX(CONVERT(float, people_fully_vaccinated_per_hundred)) as FullyVaccinationRate
FROM SQLDataExploration..CovidRecords
WHERE continent is not null AND date = '2023-05-13'
GROUP BY location
ORDER BY FullyVaccinationRate desc


-- Countries that have achieved at least 80% of people fully vaccinated
SELECT location, MAX(CONVERT(float,people_fully_vaccinated_per_hundred)) as FullyVaccinatedPerHundred
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
GROUP BY location
HAVING MAX(CONVERT(float,people_fully_vaccinated_per_hundred)) > 80
ORDER BY FullyVaccinatedPerHundred DESC


-- Using CTE (Common Table Expression) to use NumberPeopleVaccinated column in a query
With PopvsVac (continent, location, date, population, new_people_vaccinated_smoothed, NumberPeopleVaccinated)
as (
SELECT continent, location, date, population, new_people_vaccinated_smoothed, SUM(CONVERT(int, new_people_vaccinated_smoothed)) OVER (Partition by location
ORDER by date) as NumberPeopleVaccinated
FROM SQLDataExploration..CovidRecords
WHERE continent is not null
)
SELECT*, (NumberPeopleVaccinated/population)*100 as VaccinationPercentage
FROM PopvsVac


-- Using temp table to use NumberPeopleVaccinated column in a query
Create table #PercentPopulationVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_people_vaccinated_smoothed numeric,
NumberPeopleVaccinated numeric
)

INSERT into #PercentPopulationVaccinated
SELECT continent, location, date, population, new_people_vaccinated_smoothed, SUM(CONVERT(int, new_people_vaccinated_smoothed)) OVER (Partition by location
ORDER by date) as NumberPeopleVaccinated
FROM SQLDataExploration..CovidRecords
WHERE continent is not null

SELECT*, (NumberPeopleVaccinated/population)*100 as VaccinationPercentage
FROM #PercentPopulationVaccinated
ORDER BY location, date

DROP table #PercentPopulationVaccinated


-- Create view to store daily and cumulative COVID-19 vaccination data by location for later visualization
CREATE View PercentPopulationVaccinated as
SELECT continent, location, date, population, new_people_vaccinated_smoothed, SUM(CONVERT(int, new_people_vaccinated_smoothed)) OVER (Partition by location
ORDER by date) as NumberPeopleVaccinated
FROM SQLDataExploration..CovidRecords
WHERE continent is not null

SELECT *
FROM PercentPopulationVaccinated

-----------------------------------------------Recap---------------------------------------------------
SELECT location, date, new_cases, new_deaths, CONVERT(int, new_vaccinations) as NewVaccination, reproduction_rate
FROM SQLDataExploration..CovidRecords
WHERE location = 'Indonesia'
ORDER BY 1,2

-- Global numbers on spesific date
CREATE PROCEDURE GlobalRecap @date datetime
AS
SELECT location, date, total_cases, total_deaths, total_vaccinations
FROM SQLDataExploration..CovidRecords
WHERE location = 'World' AND date = @date
GO

EXEC GlobalRecap @date = '2023-04-01'

DROP PROCEDURE GlobalRecap;
GO

