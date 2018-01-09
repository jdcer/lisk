/**
 * Rename Transactions Ids attributes to addresses
 */

BEGIN;

-- Rename trs columns
ALTER TABLE "trs" RENAME "senderId" TO "senderAddress";
ALTER TABLE "trs" RENAME "recipientId" TO "recipientAddress";

-- Rename trs indexes
ALTER INDEX trs_recipient_id RENAME TO trs_recipient_address;
ALTER INDEX trs_sender_id RENAME TO trs_sender_address;
ALTER INDEX trs_upper_recipient_id RENAME TO trs_upper_recipient_address;
ALTER INDEX trs_upper_sender_id RENAME TO trs_upper_sender_address;

-- Recreate view full_blocks_list
DROP VIEW IF EXISTS full_blocks_list;
CREATE VIEW full_blocks_list AS
SELECT b."id" AS "b_id",
       b."version" AS "b_version",
       b."timestamp" AS "b_timestamp",
       b."height" AS "b_height",
       b."previousBlock" AS "b_previousBlock",
       b."numberOfTransactions" AS "b_numberOfTransactions",
       (b."totalAmount")::bigint AS "b_totalAmount",
       (b."totalFee")::bigint AS "b_totalFee",
       (b."reward")::bigint AS "b_reward",
       b."payloadLength" AS "b_payloadLength",
       ENCODE(b."payloadHash", 'hex') AS "b_payloadHash",
       ENCODE(b."generatorPublicKey", 'hex') AS "b_generatorPublicKey",
       ENCODE(b."blockSignature", 'hex') AS "b_blockSignature",
       t."id" AS "t_id",
       t."rowId" AS "t_rowId",
       t."type" AS "t_type",
       t."timestamp" AS "t_timestamp",
       ENCODE(t."senderPublicKey", 'hex') AS "t_senderPublicKey",
       t."senderAddress" AS "t_senderAddress",
       t."recipientAddress" AS "t_recipientAddress",
       (t."amount")::bigint AS "t_amount",
       (t."fee")::bigint AS "t_fee",
       ENCODE(t."signature", 'hex') AS "t_signature",
       ENCODE(t."signSignature", 'hex') AS "t_signSignature",
       ENCODE(s."publicKey", 'hex') AS "s_publicKey",
       d."name" AS "d_username",
       v."votes" AS "v_votes",
       m."min" AS "m_min",
       m."lifetime" AS "m_lifetime",
       m."keysgroup" AS "m_keysgroup",
       dapp."name" AS "dapp_name",
       dapp."description" AS "dapp_description",
       dapp."tags" AS "dapp_tags",
       dapp."type" AS "dapp_type",
       dapp."link" AS "dapp_link",
       dapp."category" AS "dapp_category",
       dapp."icon" AS "dapp_icon",
       it."dappId" AS "in_dappId",
       ot."dappId" AS "ot_dappId",
       ot."outTransactionId" AS "ot_outTransactionId",
       ENCODE(t."requesterPublicKey", 'hex') AS "t_requesterPublicKey",
       CONVERT_FROM(tf."data", 'utf8') AS "tf_data",
       t."signatures" AS "t_signatures"
FROM blocks b
LEFT OUTER JOIN trs AS t ON t."blockId" = b."id"
LEFT OUTER JOIN delegates AS d ON d."tx_id" = t."id"
LEFT OUTER JOIN votes AS v ON v."transactionId" = t."id"
LEFT OUTER JOIN signatures AS s ON s."transactionId" = t."id"
LEFT OUTER JOIN multisignatures AS m ON m."transactionId" = t."id"
LEFT OUTER JOIN dapps AS dapp ON dapp."transactionId" = t."id"
LEFT OUTER JOIN intransfer AS it ON it."transactionId" = t."id"
LEFT OUTER JOIN outtransfer AS ot ON ot."transactionId" = t."id"
LEFT OUTER JOIN transfer AS tf ON tf."transactionId" = t."id";

-- Recreate view trs_list
DROP VIEW IF EXISTS trs_list;
CREATE VIEW trs_list AS
SELECT t."id" AS "t_id",
       b."height" AS "b_height",
       t."blockId" AS "t_blockId",
       t."type" AS "t_type",
       t."timestamp" AS "t_timestamp",
       t."senderPublicKey" AS "t_senderPublicKey",
       m."publicKey" AS "m_recipientPublicKey",
       UPPER(t."senderAddress") AS "t_senderAddress",
       UPPER(t."recipientAddress") AS "t_recipientAddress",
       t."amount" AS "t_amount",
       t."fee" AS "t_fee",
       ENCODE(t."signature", 'hex') AS "t_signature",
       ENCODE(t."signSignature", 'hex') AS "t_SignSignature",
       t."signatures" AS "t_signatures",
       (SELECT height + 1 FROM blocks ORDER BY height DESC LIMIT 1) - b."height" AS "confirmations"
FROM trs t
LEFT JOIN blocks b ON t."blockId" = b."id"
LEFT JOIN mem_accounts m ON t."recipientAddress" = m."address";

-- Recretate function validateMemBalances()
DROP FUNCTION validateMemBalances();
CREATE FUNCTION validateMemBalances() RETURNS TABLE(address VARCHAR(22), pk TEXT, username VARCHAR(20), blockchain BIGINT, memory BIGINT, diff BIGINT) LANGUAGE PLPGSQL AS $$
BEGIN
	 RETURN QUERY
		WITH balances AS (
			(SELECT UPPER("senderAddress") AS address, -SUM(amount+fee) AS amount FROM trs GROUP BY UPPER("senderAddress"))
				UNION ALL
			(SELECT UPPER("recipientAddress") AS address, SUM(amount) AS amount FROM trs WHERE "recipientAddress" IS NOT NULL GROUP BY UPPER("recipientAddress"))
				UNION ALL
			(SELECT a.address, r.amount FROM
				(SELECT r.pk, SUM(r.fees) + SUM(r.reward) AS amount FROM rounds_rewards r GROUP BY r.pk) r LEFT JOIN mem_accounts a ON r.pk = a."publicKey"
			)
		),
		accounts AS (SELECT b.address, SUM(b.amount) AS balance FROM balances b GROUP BY b.address)
		SELECT m.address, ENCODE(m."publicKey", 'hex') AS pk, m.username, a.balance::BIGINT AS blockchain, m.balance::BIGINT AS memory, (m.balance-a.balance)::BIGINT AS diff
		FROM accounts a LEFT JOIN mem_accounts m ON a.address = m.address WHERE a.balance <> m.balance;
END $$;

-- Recretate function vote_insert()
DROP FUNCTION vote_insert() CASCADE;
CREATE FUNCTION vote_insert() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
	BEGIN
		INSERT INTO votes_details
		SELECT r.tx_id, r.voter_address, (CASE WHEN substring(vote, 1, 1) = '+' THEN 'add' ELSE 'rem' END) AS type, r.timestamp, r.height, DECODE(substring(vote, 2), 'hex') AS delegate_pk FROM (
			SELECT v."transactionId" AS tx_id, t."senderAddress" AS voter_address, b.timestamp AS timestamp, b.height, regexp_split_to_table(v.votes, ',') AS vote
			FROM votes v, trs t, blocks b WHERE v."transactionId" = NEW."transactionId" AND v."transactionId" = t.id AND b.id = t."blockId"
		) AS r ORDER BY r.timestamp ASC;
	RETURN NULL;
END $$;

-- Recreate trigger vote_insert;
CREATE TRIGGER vote_insert
	AFTER INSERT ON votes
	FOR EACH ROW
	EXECUTE PROCEDURE vote_insert();

-- Recretate function delegates_voters_balance_update()
DROP FUNCTION delegates_voters_balance_update();
CREATE FUNCTION delegates_voters_balance_update() RETURNS TABLE(updated INT) LANGUAGE PLPGSQL AS $$
	BEGIN
		RETURN QUERY
			WITH
			last_round AS (SELECT (CASE WHEN height < 101 THEN 1 ELSE height END) AS height FROM blocks WHERE height % 101 = 0 OR height = 1 ORDER BY height DESC LIMIT 1),
			current_round_txs AS (SELECT t.id FROM trs t LEFT JOIN blocks b ON b.id = t."blockId" WHERE b.height > (SELECT height FROM last_round)),
			voters AS (SELECT DISTINCT ON (voter_address) voter_address FROM votes_details),
			balances AS (
				(SELECT UPPER("senderAddress") AS address, -SUM(amount+fee) AS amount FROM trs GROUP BY UPPER("senderAddress"))
					UNION ALL
				(SELECT UPPER("senderAddress") AS address, SUM(amount+fee) AS amount FROM trs WHERE id IN (SELECT * FROM current_round_txs) GROUP BY UPPER("senderAddress"))
					UNION ALL
				(SELECT UPPER("recipientAddress") AS address, SUM(amount) AS amount FROM trs WHERE "recipientAddress" IS NOT NULL GROUP BY UPPER("recipientAddress"))
					UNION ALL
				(SELECT UPPER("recipientAddress") AS address, -SUM(amount) AS amount FROM trs WHERE id IN (SELECT * FROM current_round_txs) AND "recipientAddress" IS NOT NULL GROUP BY UPPER("recipientAddress"))
					UNION ALL
				(SELECT d.address, d.fees+d.rewards AS amount FROM delegates d)
			),
			filtered AS (SELECT * FROM balances WHERE address IN (SELECT * FROM voters)),
			accounts AS (SELECT b.address, SUM(b.amount) AS balance FROM filtered b GROUP BY b.address),
			updated AS (UPDATE delegates SET voters_balance = balance FROM
			(SELECT d.pk, (
				(SELECT COALESCE(SUM(balance), 0) AS balance FROM accounts WHERE address IN 
					(SELECT v.voter_address FROM
						(SELECT DISTINCT ON (voter_address) voter_address, type FROM votes_details
							WHERE delegate_pk = d.pk AND height <= (SELECT height FROM last_round)
							ORDER BY voter_address, timestamp DESC
						) v
						WHERE v.type = 'add'
					)
				)
			) FROM delegates d) dd WHERE delegates.pk = dd.pk RETURNING 1)
			SELECT COUNT(1)::INT FROM updated;
END $$;

COMMIT;
