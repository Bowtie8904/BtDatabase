package bt.db.func.impl;

import bt.db.func.SqlFunction;

/**
 * The RANDOM function returns a DOUBLE PRECISION number with positive sign, greater than or equal to zero (0), and less
 * than one (1.0).
 * 
 * @author &#8904
 */
public class RandomFunction extends SqlFunction<RandomFunction>
{
    private int seed = -1;

    public RandomFunction()
    {
        super("random");
    }

    public RandomFunction(int seed)
    {
        super("rand");
        this.seed = seed;
    }

    @Override
    public String toString()
    {
        return this.name + "(" + (this.seed == -1 ? "" : this.seed) + ")";
    }
}