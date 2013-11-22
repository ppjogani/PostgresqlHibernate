package postgreHibernateClient;

import java.util.HashMap;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import edu.usc.bg.base.ByteIterator;

@Entity
@Table(name="Student_Info")
public class Student_Info {
	@Id
	@Column(name="ROLL_NUMBER")
	private String rollno;
	
	@Column(name="NAME")
	private String name;
	
	public Student_Info(){
	}
	
	
	public String getRollno() {
		return rollno;
	}
	public void setRollno(String rollno) {
		this.rollno = rollno;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
