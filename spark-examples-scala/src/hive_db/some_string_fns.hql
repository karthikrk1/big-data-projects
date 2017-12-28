-- substr

select substr('hello world, how are you', 7, 5); // world

- instr

select instr('hello world, how are you world', world) // 7

-- like

select * from karthik_db.orders where order_status like 'PENDING%';

-- rlike

select * from karthik_db.orders where order_date rlike '[0-9]+';

-- length

select length(distinct order_status) from orders;

-- lower/lcase

select lower('HELLO'), lcase('WORLD'); // hello, world

-- ucase/upper

select upper('hello'), ucase('world'); // HELLO WORLD

-- initcap

select initcap('hello'); / Hello

-- trim

select trim('hello world '); // 'hello world'

-- ltrim and rtrim

select ltrim(' hello'), rtrim(' hello world '); // 'hello' ' hello world'

-- lpad

select lpad('20', 3, '0'); // 020

-- cast

select cast('20' as int); // 20: int

-- split

select split('Hello world, how are you', ' ');

// OUTPUT: ['Hello', 'world,', 'how', 'are', 'you'] > Array

