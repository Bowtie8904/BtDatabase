package bt.db.constants;

import bt.db.statement.clause.Column;

/**
 * Offers values to define the identity generation behavior on {@link Column}s.
 * 
 * @author &#8904
 */
public enum Generated
{
    /**
     * Indicates that an identity columns value should always be uniquely generated. This means that no value can be
     * explicitly inserted/updated.
     */
    ALWAYS,
    /**
     * Indicates that an identity columns value should only be generated if it was not inserted explicitly. This means
     * that uniqueness is not guaranteed.
     */
    DEFAULT
}