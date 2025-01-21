SELECT №, 
Дата, 
SUM(`Сумма платежа`) `Сумма платежа`, 
SUM(`Платеж по процентам`) `Платеж по процентам`, 
round(MAX(`Долг`),2) Долг120,
round(`Проценты`,2) Проценты120 
FROM
(SELECT 
IFNULL(№, LAG(№) OVER()) AS №, 
Дата,
`Сумма платежа`,
`Платеж по основному долгу`,
`Платеж по процентам`,
`Остаток долга`,
SUM(`Платеж по основному долгу`) over( rows BETWEEN UNBOUNDED PRECEDING AND current ROW ) `Долг`,
SUM(`Платеж по процентам`) over( rows BETWEEN UNBOUNDED PRECEDING AND current ROW ) `Проценты`
FROM tasketl4b20) tab1
GROUP BY 1,2,6
ORDER BY 1