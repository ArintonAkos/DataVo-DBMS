using Server.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement
{
    internal class Operator
    {
        private Operators Operand { get; set; }

        public Operator(Operators operand)
        {
            Operand = operand;
        }

        public Operator FromRaw(string operatorRaw)
        {
            try
            {
                Operators operand = (Operators)Enum.Parse(typeof(Operators), operatorRaw);

                return new Operator(operand);
            }
            catch
            {
                throw new Exception($"Unsupported operand: {operatorRaw}!");
            }
        }

        public Boolean ValidateConditions(ConditionTree conditionOne, ConditionTree conditionTwo)
        {
            switch (Operand)
            {
                case Operators.AND:
                    return conditionOne.Validate() && conditionTwo.Validate();
                default:
                    return conditionOne.Validate() || conditionTwo.Validate();
            }
        }
    }
}
