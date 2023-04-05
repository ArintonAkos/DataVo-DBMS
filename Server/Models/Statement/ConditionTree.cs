using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement
{
    internal class ConditionTree
    {
        public List<ConditionTree> ChildConditions { get; set; }
        public List<Operator> Operators { get; set; }

        public static ConditionTree FromMatch(string rawConditions)
        {
            // Write the condition parsing for sql syntax
            // This is a recursive function
            // The first condition is the root condition
            // The rest of the conditions are the child conditions
            // The child conditions are the conditions that are connected by AND or OR
            // The child conditions can be nested multiple times and have their own child conditions

            return new ConditionTree
            {
                ChildConditions = new(),
                Operators = new()
            };
        }

        public Boolean Validate()
        {
            
        }
    }
}
