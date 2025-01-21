SELECT *
FROM layoffs;

-- Duplicate table creation

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;


-- Duplicates Deletion

WITH duplicate_cte AS
(
	SELECT *,
	ROW_NUMBER() 
	OVER(
		PARTITION BY company,
					location,
					industry,
					total_laid_off,
					percentage_laid_off,
					`date`,
                    stage,
                    country,
                    funds_raised_millions
		) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() 
	OVER(
		PARTITION BY company,
					location,
					industry,
					total_laid_off,
					percentage_laid_off,
					`date`,
                    stage,
                    country,
                    funds_raised_millions
		) AS row_num
FROM layoffs_staging;
    
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


-- Data Standardization

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT company
FROM layoffs_staging2;

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country like 'United States%'; 

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;


-- NULL and Blank Values

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry,t2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
	AND T1.location = T2.location
WHERE (T1.industry IS NULL 
	OR T1.industry = '')
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
	AND T1.location = T2.location
SET T1.industry = T2.industry
WHERE T1.industry IS NULL
AND T2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company like 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Remove Unnecessary Columns

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
















