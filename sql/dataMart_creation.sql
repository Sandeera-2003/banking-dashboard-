USE DWBI_Bank;
GO

CREATE TABLE mart.marketing_performance (
    month_key TINYINT NOT NULL,
    job VARCHAR(50) NOT NULL,
    channel_key TINYINT NOT NULL,
    contacts INT NOT NULL,
    subscriptions INT NOT NULL,
    total_duration_seconds BIGINT NOT NULL,
    PRIMARY KEY (month_key, job, channel_key)
);
GO

INSERT INTO mart.marketing_performance
    (month_key, job, channel_key, contacts, subscriptions, total_duration_seconds)
SELECT
    f.month_key,
    p.job,
    f.channel_key,
    SUM(CAST(f.contact_count AS INT)),
    SUM(CAST(f.is_subscribed AS INT)),
    SUM(CAST(f.duration_seconds AS BIGINT))
FROM dw.fact_contact AS f
JOIN dw.dim_customer_profile AS p ON f.profile_key = p.profile_key
GROUP BY f.month_key, p.job, f.channel_key;
GO

SELECT SUM(contacts) AS contacts, SUM(subscriptions) AS subscriptions
FROM mart.marketing_performance;