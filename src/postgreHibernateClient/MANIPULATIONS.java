package postgreHibernateClient;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Index;


@Entity
@Table(name="MANIPULATIONS")
public class MANIPULATIONS {
	@Id
	private String mid;
	
	
	@Index(name="MANIPULATION_CREATORID")
	@Column(name="creatorid", nullable=false)
	private String creatorid;

	@Index(name="MANIPULATION_RID")
	@Column(name="rid", nullable=false)
	private String rid;
	
	@Column(name="modifierid", nullable=false)
	private String modifierid;
	
	private String timestamp;
	
	private String type;
	
	private String content;

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getCreatorid() {
		return creatorid;
	}

	public void setCreatorid(String creatorid) {
		this.creatorid = creatorid;
	}

	public String getRid() {
		return rid;
	}

	public void setRid(String rid) {
		this.rid = rid;
	}

	public String getModifierid() {
		return modifierid;
	}

	public void setModifierid(String modifierid) {
		this.modifierid = modifierid;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
}