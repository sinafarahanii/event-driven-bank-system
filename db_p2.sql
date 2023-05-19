create extension pgcrypto;
create table account(
    username    varchar(250),
    accountNumber   numeric(16, 0) unique not null,
    password    varchar(250) not null,
    first_name  varchar(50) not null,
    last_name   varchar(50) not null,
    national_id varchar(10) not null,
    date_of_birth   date,
    type    varchar(10),
    interest_rate   numeric(4, 2) check ( case when type = 'employee' then interest_rate = 0 end),
    check(type in('client', 'employee'))
);
create table login_log(
    username    varchar(250),
    login_time  timestamp,
    foreign key (username) references account(username)
 );

create table transactions(
    type    varchar(10) not null ,
    transaction_time    timestamp,
    "from"    numeric(16),
    "to"  numeric(16),
    amount  float(2),
    foreign key ("from") references account(accountNumber),
    foreign key ("to") references account(accountNumber),
    check (type in('deposit', 'withdraw', 'transfer', 'interest'))
);

create table latest_balances(
    accountNumber   numeric(16),
    amount  float(2),
    foreign key (accountNumber) references account(accountNumber)
);

create table snapshot_log(
    snapshot_id numeric(4),
    snapshot_timestamp  timestamp
);
create function username_changing() returns trigger as $username_making$
    begin
        if new.username is null then
            new.username = concat(new.first_name, new.last_name);
        end if;
        return new;
    end
    $username_making$ language plpgsql;

create trigger username_making before insert on account
    for each row execute function username_changing();

create procedure register(in password varchar(18), in firstname varchar(50),
                          in lastname varchar(50), in nationalid varchar(10), in dateofbirth_var varchar(20), in type varchar(10),
                          in interestrate int, out respond int)
    language plpgsql
    as $$
    declare age integer;
    declare account_Number numeric(16);
    declare hashed_password varchar(250);
    declare dateofbirth timestamp;
    begin
        SELECT to_timestamp(
                       concat(dateofbirth_var,' ',localtime(0)),
                       'YYYY-MM-DD HH24:MI:SS'
                   ) into dateofbirth;
        select date_part('year', age(current_date, dateofbirth)) into age;
        if age < 13 then
            select 0 into respond;
        else
            select CAST(1000000000000000 + floor(random() * 9000000000000000) AS bigint) into account_Number;
            hashed_password = crypt(password, gen_salt('bf'));
            if type='employee' then
                insert into account values (null, account_Number, hashed_password, firstname, lastname, nationalid, dateofbirth, 'employee', 0);
                select 1 into respond;
            elseif type='client' then
                insert into account values (null, account_Number, hashed_password, firstname, lastname, nationalid, dateofbirth, 'client', interestrate);
                select 1 into respond;
            else
                select -1 into respond;
            end if;
            insert into latest_balances values (account_number, 0);
        end if;
    end $$;

create procedure login(in input_username varchar(250), in input_password varchar(18), out respond int)
    language plpgsql
    as $$
    declare user_password varchar(250);
    begin
        select account.password into user_password
        from account
        where account.username = input_username;
        if user_password = crypt(input_password, user_password) then
            insert into login_log(username, login_time) values (input_username, current_timestamp);
            select 1 into respond;

        else
            select -1 into respond;
        end if;
    end $$;
create procedure deposit(in deposit_amount float(2))
    language plpgsql
    as $$
    declare active_user_accountNumber numeric(16, 0);
    begin
        select account.accountNumber into active_user_accountNumber
        from account
        where  account.username = ( select login_log.username
                                    from login_log
                                    order by login_time desc
                                    limit 1);
        insert into transactions(type, transaction_time, "from", "to", amount) values ('deposit', current_timestamp, null, active_user_accountNumber, deposit_amount);

    end $$;
create procedure withdraw(in withdraw_amount float(2))
    language plpgsql
    as $$
    declare active_user_accountNumber numeric(16, 0);
    begin
        select account.accountNumber into active_user_accountNumber
        from account
        where  account.username = ( select login_log.username
                                    from login_log
                                    order by login_time desc
                                    limit 1);
        insert into transactions(type, transaction_time, "from", "to", amount) values ('withdraw', current_timestamp, active_user_accountNumber, null, withdraw_amount);

    end $$;
create procedure transfer(in transfer_amount float(2), in destination numeric(16, 0), out result int)
    language plpgsql
    as $$
    declare active_user_accountNumber numeric(16, 0);
    begin
        if destination in (select accountNumber
                           from account) then
            select account.accountNumber into active_user_accountNumber
            from account
            where  account.username = ( select login_log.username
                                        from login_log
                                        order by login_time desc
                                        limit 1);
            insert into transactions(type, transaction_time, "from", "to", amount) values ('transfer', current_timestamp, active_user_accountNumber, destination, transfer_amount);
            select 1 into result;
        else
            select -1 into result;
        end if;
    end $$;


create procedure interest_payment()
    language plpgsql
    as $$
    declare active_user_accountNumber numeric(16, 0);
    declare active_user account;
    declare account_interest numeric(4);
    begin
        select * into active_user
        from account
        where  account.username = ( select login_log.username
                                    from login_log
                                    order by login_time desc
                                    limit 1);
        select active_user.accountNumber into active_user_accountNumber;
        select active_user.interest_rate/100 into account_interest;
        insert into transactions(type, transaction_time, "from", "to", amount) values('interest', current_timestamp, null, active_user_accountNumber, account_interest);

    end $$;

create procedure update_balances(out respond int)
    language plpgsql
    as $$
    declare last_snapshot_time timestamp;
    declare last_snapshot record;
    declare f transactions;
    declare account_current_amount float(2);
    declare current_account record;
    declare user_role varchar(10);
    declare current_snapshot_id numeric(4);
    declare user_type varchar(10);
    declare snapshot_table varchar(20);
    begin
        select type into user_type
        from account
        where  account.username = ( select login_log.username
                                    from login_log
                                    order by login_time desc
                                    limit 1);
        if user_type = 'client' then
            select -999 into respond;
        else
        if exists(select * from snapshot_log) then
        select * into last_snapshot
        from snapshot_log
        order by snapshot_log desc
        limit 1;
        select last_snapshot.snapshot_timestamp into last_snapshot_time;
        select last_snapshot.snapshot_id into current_snapshot_id;
        else
            select 1000 into current_snapshot_id;
            SELECT TO_TIMESTAMP('2017-03-31 9:30:20','YYYY-MM-DD HH:MI:SS') into last_snapshot_time;
        end if;
    for f in select *
        from transactions
        where transaction_time > last_snapshot_time
        order by transaction_time
        loop
            if f.type = 'deposit' then
                update latest_balances
                set amount = latest_balances.amount + f.amount
                where latest_balances.accountNumber = f."to";
            elseif f.type = 'withdraw' then

                select * into current_account
                from latest_balances natural join account
                where latest_balances.accountNumber = f."from";

                select current_account.amount into account_current_amount;
                select current_account.type into user_role;
                if account_current_amount - f.amount < 0 and user_role != 'employee' then
                    select -1 into respond;
                else
                    update latest_balances
                    set amount = latest_balances.amount - f.amount
                    where latest_balances.accountNumber = f."from";
                end if;
                account_current_amount = 0;
            elseif f.type = 'transfer' then
                select latest_balances.amount into account_current_amount
                from latest_balances
                where latest_balances.accountNumber = f."from";
                if account_current_amount - f.amount < 0 and user_role != 'employee' then
                    select -1 into respond;
                else
                    update latest_balances
                    set amount = latest_balances.amount - f.amount
                    where latest_balances.accountNumber = f."from";
                    update latest_balances
                    set amount = latest_balances.amount + f.amount
                    where latest_balances.accountNumber = f."to";
                end if;
            elseif f.type = 'interest' then
                update latest_balances
                set amount = latest_balances.amount + f.amount * latest_balances.amount
                where latest_balances.accountNumber = f."to";
            end if;
        end loop;
        current_snapshot_id = current_snapshot_id+1;
        snapshot_table = concat('snapshot_', current_snapshot_id);
        insert into snapshot_log values(current_snapshot_id ,current_timestamp);
        execute format('create table %I as table latest_balances', snapshot_table);
    end if;
    end $$;

create procedure check_balance(out respond float(2))
    language plpgsql
    as $$
    declare active_user_accountNumber numeric(16, 0);
    begin
        select account.accountNumber into active_user_accountNumber
        from account
        where  account.username = ( select login_log.username
                                    from login_log
                                    order by login_time desc
                                    limit 1);
        select amount into respond
        from latest_balances
        where accountNumber = active_user_accountNumber;
    end $$;

--drop procedure update_balances(respond int);
--
--drop procedure login(input_username varchar(250), input_password varchar(18), respond int);
--drop procedure register(password varchar, firstname varchar, lastname varchar, nationalid varchar, dateofbirth date, type varchar, interestrate numeric, respond integer)
--drop procedure register(password varchar, firstname varchar, lastname varchar, nationalid varchar, dateofbirth_var varchar, type varchar, interestrate integer, respond integer)
--select *
--from account;
--drop table snapshot_log
--drop procedure update_balances(respond int)
--drop database postgres
--drop function username_changing cascade;
--drop trigger username_maker on account;
--insert into account values (null, '3333444455556666', '9999', 'joe', 'biden', '7788992211', '2003-02-01 11:11:11', 'client', 22);
--insert into account values('null', '123456789101652', '5273', 'ali', 'farahani', '1234567342', '1999-04-02 22:42:13', 'client', 30.00)