package caoss.simulator.instructions;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A list of instructions
 * 
 * @author Herve Paulino
 *
 */
public class InstructionList extends ArrayList<Instruction> {
	
	
	public class InstructionListIterator implements Iterator<Instruction> {

		private Iterator<Instruction> iterator;
		private final InstructionList instructionList;
		
		InstructionListIterator(InstructionList instructionList) {
			this.instructionList = instructionList;
			restart();
		}
		
		@Override
		public boolean hasNext() {
			return this.iterator.hasNext();
		}

		@Override
		public Instruction next() {
			return this.iterator.next();
		}

		@Override
		public void remove() {
			this.iterator.remove();
		}
		
		public void restart() {
			this.iterator = this.instructionList.baseIterator();
		}
		
	}
	
	/**
	 * Default serial version id
	 */
	private static final long serialVersionUID = 1L;

	
	public InstructionListIterator iterator() {
		return new InstructionListIterator(this);
	}
	
	private Iterator<Instruction> baseIterator() {
		return super.iterator();
	}
	

}
