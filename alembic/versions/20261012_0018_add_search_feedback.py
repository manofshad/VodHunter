"""Add current successful-match ratings and a separate feedback reporting view."""

from alembic import op


revision = "20261012_0018"
down_revision = "20261011_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE search_requests
        ADD COLUMN feedback_rating TEXT,
        ADD COLUMN feedback_updated_at TIMESTAMPTZ,
        ADD CONSTRAINT search_requests_feedback_check CHECK (
            (feedback_rating IS NULL AND feedback_updated_at IS NULL)
            OR (feedback_rating IS NOT NULL AND feedback_rating IN ('up', 'down')
                AND feedback_updated_at IS NOT NULL
                AND source_app IS NOT DISTINCT FROM 'public'
                AND job_status IS NOT DISTINCT FROM 'completed'
                AND success IS TRUE AND found_match IS TRUE)
        )
    """)
    op.execute("""
        CREATE INDEX ix_search_requests_feedback_updated_at
        ON search_requests (feedback_updated_at)
        WHERE feedback_rating IS NOT NULL
    """)
    op.execute("""
        CREATE VIEW grafana_search_feedback AS
        SELECT quality.*,
               request.feedback_rating,
               request.feedback_updated_at,
               request.tiktok_url,
               request.matched_timestamp_seconds,
               request.result_payload,
               request.source_app,
               request.route,
               request.input_type,
               request.download_source,
               request.download_host,
               request.input_duration_seconds,
               request.success,
               request.http_status,
               request.error_message
        FROM grafana_search_quality AS quality
        JOIN search_requests AS request ON request.id = quality.search_id
        WHERE request.feedback_rating IS NOT NULL
    """)
    op.execute("""
        DO $$
        DECLARE permission RECORD;
        BEGIN
            FOR permission IN
                SELECT acl.grantee, acl.is_grantable, role.rolname
                FROM pg_class AS relation
                CROSS JOIN LATERAL aclexplode(relation.relacl) AS acl
                LEFT JOIN pg_roles AS role ON role.oid = acl.grantee
                WHERE relation.oid = 'grafana_search_quality'::regclass
                  AND acl.privilege_type = 'SELECT'
                  AND acl.grantee <> relation.relowner
            LOOP
                EXECUTE format('GRANT SELECT ON grafana_search_feedback TO %s%s',
                    CASE WHEN permission.grantee = 0 THEN 'PUBLIC'
                         ELSE quote_ident(permission.rolname) END,
                    CASE WHEN permission.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
            END LOOP;
        END $$
    """)


def downgrade() -> None:
    op.execute("DROP VIEW grafana_search_feedback")
    op.execute("DROP INDEX ix_search_requests_feedback_updated_at")
    op.execute("""
        ALTER TABLE search_requests
        DROP CONSTRAINT search_requests_feedback_check,
        DROP COLUMN feedback_rating,
        DROP COLUMN feedback_updated_at
    """)
