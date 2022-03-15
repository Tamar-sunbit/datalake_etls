DROP TABLE IF EXISTS {schem}.{prefix}_card;
DROP TABLE IF EXISTS {schem}.{prefix}_accounts;
DROP TABLE IF EXISTS {schem}.{prefix}_applications;
DROP TABLE IF EXISTS {schem}.{prefix}_invitations;
DROP TABLE IF EXISTS {schem}.{prefix}_customers;

CREATE TABLE {schem}.{prefix}_customers (
	customer_id          integer  NOT NULL  ,
	eligibility_start_date timestamp    ,
	entitlement          boolean    ,
	not_entitlement_reason varchar(50)    ,
	entitlement_date     timestamp    ,
	unsubscribe_flag     boolean DEFAULT FALSE   ,
	unsubscribe_date     timestamp    ,
	unsubscribe_reason   varchar(100)    ,
	CONSTRAINT pk_eligibility PRIMARY KEY ( customer_id )
 );


 CREATE TABLE {schem}.{prefix}_invitations (
	invitation_id        integer  NOT NULL  ,
	customer_id          integer  NOT NULL   ,
	invitation_date      timestamp  NOT NULL  ,
	campaign_id          integer    ,
	workflow_id          integer    ,
	opened_flag          boolean DEFAULT FALSE NOT NULL  ,
	open_date            timestamp    ,
	clicked_flag         boolean DEFAULT FALSE NOT NULL   ,
	clicked_date         timestamp    ,
	last_purchase_date   timestamp    ,
	last_purchase_vertical varchar(50)    ,
	last_purchase_risk_tier integer    ,
	CONSTRAINT pk_invitations PRIMARY KEY ( invitation_id )
 );

ALTER TABLE {schem}.{prefix}_invitations ADD CONSTRAINT fk_invitations_customers FOREIGN KEY ( customer_id ) REFERENCES {schem}.{prefix}_customers( customer_id );


CREATE TABLE {schem}.{prefix}_applications (
	application_id       integer  NOT NULL  ,
	invitation_id        integer    ,
	customer_id          integer  NOT NULL  ,
	application_start_date timestamp    ,
	application_completion_date timestamp    ,
	application_status   varchar(100)    ,
	lead_id              integer    ,
	application_complete_flag boolean DEFAULT FALSE NOT NULL  ,
	monthly_income       decimal(10,2)    ,
	monthly_housing      decimal(10,2)    ,
	housing_expense_type varchar(10)    ,
	approval_flag        boolean DEFAULT FALSE NOT NULL  ,
	approval_date        timestamp    ,
	decline_reason       varchar(50)    ,
	offer_flag           boolean DEFAULT FALSE NOT NULL  ,
	offer_date           timestamp    ,
	CONSTRAINT pk_application PRIMARY KEY ( application_id )
 );

ALTER TABLE {schem}.{prefix}_applications ADD CONSTRAINT fk_applications_customers FOREIGN KEY ( customer_id ) REFERENCES {schem}.{prefix}_customers( customer_id );
ALTER TABLE {schem}.{prefix}_applications ADD CONSTRAINT fk_applications_invitations FOREIGN KEY ( invitation_id ) REFERENCES {schem}.{prefix}_invitations( invitation_id );

CREATE TABLE {schem}.{prefix}_accounts (
	account_id           integer  NOT NULL  ,
	customer_id          integer  NOT NULL  ,
	application_id       integer  NOT NULL  ,
	first_activation_flag boolean DEFAULT FALSE NOT NULL  ,
	first_activation_date timestamp    ,
	is_first_transaction boolean DEFAULT FALSE NOT NULL  ,
	first_transaction_date timestamp    ,
	status               varchar(10)    ,
	CONSTRAINT pk_application_0 PRIMARY KEY ( account_id )
 );

ALTER TABLE {schem}.{prefix}_accounts ADD CONSTRAINT fk_accounts_applications FOREIGN KEY ( application_id ) REFERENCES {schem}.{prefix}_applications( application_id );
ALTER TABLE {schem}.{prefix}_accounts ADD CONSTRAINT fk_accounts_customers FOREIGN KEY ( customer_id ) REFERENCES {schem}.{prefix}_customers( customer_id );


CREATE TABLE {schem}.{prefix}_card (
	card_number          integer  NOT NULL  ,
	account_id           integer  NOT NULL  ,
	customer_id          integer  NOT NULL  ,
	sent_flag            boolean DEFAULT FALSE NOT NULL  ,
	sent_date            timestamp    ,
	booked_flag          boolean DEFAULT FALSE NOT NULL  ,
	booked_date          timestamp    ,
	card_activated_flag  boolean DEFAULT FALSE NOT NULL  ,
	card_activated_date  timestamp    ,
	status               varchar(50)    ,
	CONSTRAINT pk_cards PRIMARY KEY ( card_number )
 );
ALTER TABLE {schem}.{prefix}_card ADD CONSTRAINT fk_card_accounts FOREIGN KEY ( account_id ) REFERENCES {schem}.{prefix}_accounts( account_id );
ALTER TABLE {schem}.{prefix}_card ADD CONSTRAINT fk_card_customers FOREIGN KEY ( customer_id ) REFERENCES {schem}.{prefix}_customers( customer_id );