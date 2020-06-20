package bt.db.constants;

import bt.db.statement.clause.Column;

/**
 * Offers values to define the generation behavior on {@link Column}s.
 *
 * @author &#8904
 */
public enum Generated
{
    /**
     * Indicates that a columns value should always be generated. This means that no value can be explicitly
     * inserted/updated.
     */
    ALWAYS,
    /**
     * Indicates that a columns value should only be generated if it was not inserted explicitly. This means that
     * uniqueness for identity columns is not guaranteed.
     */
    DEFAULT
}