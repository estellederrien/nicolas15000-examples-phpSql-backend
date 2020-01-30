<?php

include_once('../../class/Query.class.php');
include_once('../../config.php'); // POUR LES LOGS

class Objectifs extends Query {
	const TABLE              = 'objectifs';
    const TABLE_APPELS       = 'historique';
    
	public function __construct(){
        parent::GetConnection();          
    }


	 public function CreateCommerceObjectifs($data) {
        try {
			$result = $this->Add(self::TABLE,$data);
			
        } catch(Exception $e) {
           file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
    }
	
	
	
	public function testPresenceId($data) {
        try {
			
			
			 $sql = 'SELECT * FROM ' . self::TABLE . ' WHERE id_myProg_utilisateurs = '.$data[id_myProg_utilisateurs].' AND annee = '.$data[annee];
			// Ne pas oublier ajouter annee
		

            $req = $this->bdd->prepare($sql);
            $req->execute();

            $result = $req->Fetch();
            $req->closeCursor();
		
			if($result){
				
				return true;
				
			}else{
				
				return false;
			}
			
        } catch(Exception $e) {
            file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
    }
	
	 public function ReadCommerceObjectifs($filtres){

        try {
			
			
			
			
			if($filtres->id_myProg_utilisateurs){
				$sql = 'SELECT * FROM ' . self::TABLE . '  WHERE id_myProg_utilisateurs = '.$filtres->id_myProg_utilisateurs.' AND annee = '.$filtres->annee ;
			}else{
				$sql = 'SELECT 
				SUM(ca_janvier) 	AS ca_janvier,
				SUM(ca_fevrier) 	AS ca_fevrier,
				SUM(ca_mars) 		AS ca_mars,
				SUM(ca_avril) 		AS ca_avril,
				SUM(ca_mai) 		AS ca_mai,
				SUM(ca_juin) 		AS ca_juin,
				SUM(ca_juillet) 	AS ca_juillet,
				SUM(ca_aout) 		AS ca_aout,
				SUM(ca_septembre) 	AS ca_septembre,
				SUM(ca_octobre) 	AS ca_octobre,
				SUM(ca_novembre) 	AS ca_novembre,
				SUM(ca_decembre) 	AS ca_decembre,
				SUM(ca_janvier+ca_fevrier+ca_mars+ca_avril+ca_mai+ca_juin+ca_juillet+ca_aout+ca_septembre+ca_octobre+ca_novembre+ca_decembre) AS ca_annee,
				SUM(volume_vente_janvier) 	AS volume_vente_janvier,
				SUM(volume_vente_fevrier) 	AS volume_vente_fevrier,
				SUM(volume_vente_mars) 		AS volume_vente_mars,
				SUM(volume_vente_avril) 	AS volume_vente_avril,
				SUM(volume_vente_mai) 		AS volume_vente_mai,
				SUM(volume_vente_juin) 		AS volume_vente_juin,
				SUM(volume_vente_juillet) 	AS volume_vente_juillet,
				SUM(volume_vente_aout) 		AS volume_vente_aout,
				SUM(volume_vente_septembre) AS volume_vente_septembre,
				SUM(volume_vente_octobre) 	AS volume_vente_octobre,
				SUM(volume_vente_novembre) 	AS volume_vente_novembre,
				SUM(volume_vente_decembre) 	AS volume_vente_decembre,
				SUM(volume_vente_janvier+volume_vente_fevrier+volume_vente_mars+volume_vente_avril+volume_vente_mai+volume_vente_juin+volume_vente_juillet+volume_vente_aout+volume_vente_septembre+volume_vente_octobre+volume_vente_novembre+volume_vente_decembre) AS volume_vente_annee
				FROM '  . self::TABLE . '  WHERE  annee = '.$filtres->annee ;
				
				
			}


            $req = $this->bdd->prepare($sql);
            $req->execute();

            $result = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			$numArray = array_map('intval',$result);

			/* var_dump($numArray ); */
			return $numArray;

        } catch(Exception $e) {
            file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }

    }
	
	
	public function ReadCommerceObjectifsSum($filtres){

        try {
			
			$annee = date("Y");
			
			
			
			if($filtres->id_myProg_utilisateurs){
				$sql = 'SELECT 
				SUM(ca_janvier) 	AS ca_janvier,
				SUM(ca_fevrier) 	AS ca_fevrier,
				SUM(ca_mars) 		AS ca_mars,
				SUM(ca_avril) 		AS ca_avril,
				SUM(ca_mai) 		AS ca_mai,
				SUM(ca_juin) 		AS ca_juin,
				SUM(ca_juillet) 	AS ca_juillet,
				SUM(ca_aout) 		AS ca_aout,
				SUM(ca_septembre) 	AS ca_septembre,
				SUM(ca_octobre) 	AS ca_octobre,
				SUM(ca_novembre) 	AS ca_novembre,
				SUM(ca_decembre) 	AS ca_decembre,
				SUM(ca_janvier+ca_fevrier+ca_mars+ca_avril+ca_mai+ca_juin+ca_juillet+ca_aout+ca_septembre+ca_octobre+ca_novembre+ca_decembre) AS ca_annee,
				SUM(volume_vente_janvier) 	AS volume_vente_janvier,
				SUM(volume_vente_fevrier) 	AS volume_vente_fevrier,
				SUM(volume_vente_mars) 		AS volume_vente_mars,
				SUM(volume_vente_avril) 	AS volume_vente_avril,
				SUM(volume_vente_mai) 		AS volume_vente_mai,
				SUM(volume_vente_juin) 		AS volume_vente_juin,
				SUM(volume_vente_juillet) 	AS volume_vente_juillet,
				SUM(volume_vente_aout) 		AS volume_vente_aout,
				SUM(volume_vente_septembre) AS volume_vente_septembre,
				SUM(volume_vente_octobre) 	AS volume_vente_octobre,
				SUM(volume_vente_novembre) 	AS volume_vente_novembre,
				SUM(volume_vente_decembre) 	AS volume_vente_decembre,
				SUM(volume_vente_janvier+volume_vente_fevrier+volume_vente_mars+volume_vente_avril+volume_vente_mai+volume_vente_juin+volume_vente_juillet+volume_vente_aout+volume_vente_septembre+volume_vente_octobre+volume_vente_novembre+volume_vente_decembre) AS volume_vente_annee
				FROM ' . self::TABLE . '  
				WHERE id_myProg_utilisateurs = '.$filtres->id_myProg_utilisateurs.' 
				AND annee = '.$annee;
			}else{
				$sql = 'SELECT 
				SUM(ca_janvier) 	AS ca_janvier,
				SUM(ca_fevrier) 	AS ca_fevrier,
				SUM(ca_mars) 		AS ca_mars,
				SUM(ca_avril) 		AS ca_avril,
				SUM(ca_mai) 		AS ca_mai,
				SUM(ca_juin) 		AS ca_juin,
				SUM(ca_juillet) 	AS ca_juillet,
				SUM(ca_aout) 		AS ca_aout,
				SUM(ca_septembre) 	AS ca_septembre,
				SUM(ca_octobre) 	AS ca_octobre,
				SUM(ca_novembre) 	AS ca_novembre,
				SUM(ca_decembre) 	AS ca_decembre,
				SUM(ca_janvier+ca_fevrier+ca_mars+ca_avril+ca_mai+ca_juin+ca_juillet+ca_aout+ca_septembre+ca_octobre+ca_novembre+ca_decembre) AS ca_annee,
				SUM(volume_vente_janvier) 	AS volume_vente_janvier,
				SUM(volume_vente_fevrier) 	AS volume_vente_fevrier,
				SUM(volume_vente_mars) 		AS volume_vente_mars,
				SUM(volume_vente_avril) 	AS volume_vente_avril,
				SUM(volume_vente_mai) 		AS volume_vente_mai,
				SUM(volume_vente_juin) 		AS volume_vente_juin,
				SUM(volume_vente_juillet) 	AS volume_vente_juillet,
				SUM(volume_vente_aout) 		AS volume_vente_aout,
				SUM(volume_vente_septembre) AS volume_vente_septembre,
				SUM(volume_vente_octobre) 	AS volume_vente_octobre,
				SUM(volume_vente_novembre) 	AS volume_vente_novembre,
				SUM(volume_vente_decembre) 	AS volume_vente_decembre,
				SUM(volume_vente_janvier+volume_vente_fevrier+volume_vente_mars+volume_vente_avril+volume_vente_mai+volume_vente_juin+volume_vente_juillet+volume_vente_aout+volume_vente_septembre+volume_vente_octobre+volume_vente_novembre+volume_vente_decembre) AS volume_vente_annee
				FROM '  . self::TABLE . '  
				WHERE  annee = '.$annee ;
				
				
			}


            $req = $this->bdd->prepare($sql);
            $req->execute();

            $result = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			$numArray = array_map('intval',$result);

			/* var_dump($numArray ); */
			return $numArray;

        } catch(Exception $e) {
            file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }

    }
	
	/**
	 *  Update des objectifs d'un commercial 
	 *  
	 * 	@param array ( Comprends les chiffres objectifs + id-utilisateurs_myProg + annee)
	 *  @return true| false
	 */
	 
	 public function UpdateCommerceObjectifs($data){

        try {
			// echo $data[id_commerce_objectifs];
			
			$result = $this->Update(self::TABLE,$data,$data[id_commerce_objectifs]);
			if($result){
				return true;
			}
			
			
        } catch(Exception $e) {
            file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }

    }
	
	/**
	 *  Requête qui remplit le tableau de la vue Commerciaux/SuiviPersonnel/ mini fenetre SUIVI DES CHIFFRES -  COLONNE "Top tous Commerciaux"
	 *  On peut ensuite voir le meilleur resultat de vente en EUROS d'entre tous les commerciaux pour une annee donnée 
	 * 	@param annee 
	 *  @return array | false
	 */
	
	 public function GetTopMois($filtres) {
        try {
	
          $sql  = " 
				WITH monthly_sales AS (
					SELECT 
					date_trunc('month', date_vente) AS txn_month, 
					id_user,
					sum(prix_vente) as monthly_sum
					FROM crm_vente 
					WHERE 1=1 
					AND date_part('year', date_vente) = ".$filtres->annee."
					GROUP BY txn_month, id_user
					ORDER BY txn_month, id_user),

				rank_monthly_sales_by_user_id AS (
					SELECT *,
					ROW_NUMBER() OVER (PARTITION BY txn_month ORDER BY monthly_sum DESC) AS rank
					FROM monthly_sales)

				SELECT
					txn_month,
					monthly_sum
					FROM rank_monthly_sales_by_user_id
					WHERE rank = 1
					ORDER BY txn_month ASC;
		 ";

		 
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->FetchAll(PDO::FETCH_ASSOC);
            $req->closeCursor();
            return $result; 
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	/**
	 *  Requête qui remplit le tableau de la vue Commerciaux/SuiviPersonnel/ mini fenetre VOLUMES -  COLONNE "Top tous Commerciaux"
	 *  On peut ensuite voir le meilleur volume de vente en nb , d'entre tous les commerciaux pour une annee donnée 
	 * @param annee 
	 * @return array | false
	 */
	public function GetTopMoisCount($filtres) {
        try {
			
          $sql  = " 
				WITH monthly_sales AS (
					SELECT 
					date_trunc('month', date_vente) AS txn_month, 
					id_user,
					count(prix_vente) as monthly_count
					FROM crm_vente 
					WHERE 1=1 
					AND date_part('year', date_vente) = ".$filtres->annee."
					GROUP BY txn_month, id_user
					ORDER BY txn_month, id_user),

				rank_monthly_sales_by_user_id AS (
					SELECT *,
					ROW_NUMBER() OVER (PARTITION BY txn_month ORDER BY monthly_count DESC) AS rank
					FROM monthly_sales)

				SELECT
					txn_month,
					monthly_count
					FROM rank_monthly_sales_by_user_id
					WHERE rank = 1
					ORDER BY txn_month ASC;
		 ";
		
			
		 
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->FetchAll(PDO::FETCH_ASSOC);
            $req->closeCursor();
            return $result; 
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	
	/**
	 *  Requête qui remplit le tableau de la vue Commerciaux/SuiviPersonnel/ mini fenetre SUIVI DES CHIFFRES -  COLONNE "Mois référence"
	 *  On peut ensuite voir le meilleur resultat de vente en EUROS d'entre tous les commerciaux pour toutes les années
	 * 	@param id_myProg_utilisateurs
	 *  @return array | false
	 */
	
	 public function GetReference($filtres) {
        try {
	
			  if($filtres->id_myProg_utilisateurs){
				   $sql  = " 
					 SELECT s.id_user,
							s.txn_month,
							max(s.total_vente) AS max
						   FROM ( SELECT sum(crm_vente.prix_vente) AS total_vente,
									crm_vente.id_user,
									date_part('month'::text, crm_vente.date_vente) AS txn_month,
									date_part('year'::text, crm_vente.date_vente) AS txn_year
								   FROM crm_vente
								  GROUP BY crm_vente.id_user, date_part('month'::text, crm_vente.date_vente), date_part('year'::text, crm_vente.date_vente)) s
						  WHERE s.id_user = ".$filtres->id_myProg_utilisateurs."
						  GROUP BY s.id_user, s.txn_month
						  ORDER BY s.id_user, s.txn_month;

						 ";
			  }
			  else{
				 $sql  = " 
						SELECT c.numero,R.* FROM commerce_mois AS c
						LEFT OUTER JOIN

						(
						 SELECT 
							s.txn_month,
							max(s.total_vente) AS max
						   FROM ( SELECT sum(crm_vente.prix_vente) AS total_vente,
									
									date_part('month'::text, crm_vente.date_vente) AS txn_month,
									date_part('year'::text, crm_vente.date_vente) AS txn_year
								   FROM crm_vente
								  GROUP BY crm_vente.id_user, date_part('month'::text, crm_vente.date_vente), date_part('year'::text, crm_vente.date_vente)) s

						  GROUP BY  s.txn_month
						  ORDER BY  s.txn_month
						  ) R

						  ON c.numero = R.txn_month

				 ";



				
			  }
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->FetchAll(PDO::FETCH_ASSOC);
            $req->closeCursor();
            return $result; 
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	/**
	 *  Requête qui remplit le tableau de la vue Commerciaux/SuiviPersonnel/ mini fenetre SUIVI DES CHIFFRES -  COLONNE "Mois référence"
	 *  On peut ensuite voir le meilleur resultat de vente en VOLUME d'entre tous les commerciaux pour toutes les années
	 * 	@param id_myProg_utilisateurs
	 *  @return array | false
	 */
	
	 public function GetReferenceVolume($filtres) {
        try {
	
			  if($filtres->id_myProg_utilisateurs){
				   $sql  = " 
							SELECT s.id_user,
								s.txn_month,
								max(s.total_volume) AS max
							   FROM ( SELECT count(crm_vente.prix_vente) AS total_volume,
										crm_vente.id_user,
										date_part('month'::text, crm_vente.date_vente) AS txn_month,
										date_part('year'::text, crm_vente.date_vente) AS txn_year
									   FROM crm_vente
									  GROUP BY crm_vente.id_user, date_part('month'::text, crm_vente.date_vente), date_part('year'::text, crm_vente.date_vente)) s
							  WHERE s.id_user = ".$filtres->id_myProg_utilisateurs."
							  GROUP BY s.id_user, s.txn_month
							  ORDER BY s.id_user, s.txn_month;
							";
			  }
			  else{
				 $sql  = " 
					SELECT c.numero,R.* FROM commerce_mois AS c
					LEFT OUTER JOIN

					(
					 SELECT 
						s.txn_month,
						max(s.total_volume) AS max
					   FROM ( SELECT count(crm_vente.prix_vente) AS total_volume,
								
								date_part('month'::text, crm_vente.date_vente) AS txn_month,
								date_part('year'::text, crm_vente.date_vente) AS txn_year
							   FROM crm_vente
							  GROUP BY crm_vente.id_user, date_part('month'::text, crm_vente.date_vente), date_part('year'::text, crm_vente.date_vente)) s

					  GROUP BY  s.txn_month
					  ORDER BY  s.txn_month
					  ) R

					  ON c.numero = R.txn_month

				 ";



				
			  }
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->FetchAll(PDO::FETCH_ASSOC);
            $req->closeCursor();
            return $result; 
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	/**
	 *  CALCUL DU CA TOTAL, CA ENTRANT , CA SORTANT, CA OPERATION ..
	 *  VUE SUIVI PERSONNEL - FENETRE STATS DE LA PERIODE
	 * 	@param id_utilisateurs_myProg | annee
	 *  @return array | false
	 */
	public function GetCaTotal($filtres) {
        try {
		$total = [] ; // Ce tableau va contenir chaque résultats, puis on l'envoie au front end ...	on pushe dedans au fur et à mesure
		 
			
			// PHASE 1 : SOMME CA TOTAL, MOYENNE DE VENTE
			$sql  = "SELECT 
					SUM(prix_vente) 	AS ca_total,
					COUNT(prix_vente) 	AS ca_total_vol,
					AVG(prix_vente) 	AS vente_moyen,
					
					(SELECT  AVG(prix_vente) FROM crm_vente WHERE 1=1 
						AND date_part('year', date_vente) = ".$filtres->annee;
						 if ($filtres->id_myProg_utilisateurs) { 
							$sql  .= " AND id_user = ".$filtres->id_myProg_utilisateurs;
						} 
					$sql  .= ") AS vente_moyenop,
					
					(SELECT COUNT(prix_vente) FROM crm_vente WHERE modalite_restitution = 'Téléphonique' 
						AND date_part('year', date_vente) = ".$filtres->annee; 
						if ($filtres->id_myProg_utilisateurs) { 
							$sql  .= " AND id_user = ".$filtres->id_myProg_utilisateurs;
						} 
					$sql  .= ") AS nbRemiseTel,
					
					(SELECT COUNT(prix_vente) FROM crm_vente WHERE modalite_restitution = 'Sur place' 
						AND date_part('year', date_vente) = ".$filtres->annee; 
						if ($filtres->id_myProg_utilisateurs) { 
							$sql  .= " AND id_user = ".$filtres->id_myProg_utilisateurs;
						} 
					$sql  .=	") AS nbRemisePlace,
					
					(SELECT COUNT(prix_vente) FROM crm_vente WHERE modalite_restitution = 'Pas de remise' 
						AND date_part('year', date_vente) = ".$filtres->annee;
						if ($filtres->id_myProg_utilisateurs) { 
							$sql  .= " AND id_user = ".$filtres->id_myProg_utilisateurs;
						} 
						$sql  .=	") AS nbPasDeRemise
					
					FROM crm_vente 
					
					WHERE 1=1 AND date_part('year', date_vente) = ".$filtres->annee;
		
				 if ($filtres->id_myProg_utilisateurs) { 
					$sql  .= " AND id_user = ".$filtres->id_myProg_utilisateurs;
				} 
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle ce premier résultat dans le tableau :
			array_push($total,$result);
			
			// PHASE 2 : CA ENTRANT ( LE CLIENT APPELLE POUR ACHETER ) + VOLUME ENTRANT
			
			$sql  = "SELECT 
					 SUM(v.prix_vente) AS ca_entrant, 
					 COUNT(v.prix_vente) AS ca_entrant_vol 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'Entrant'
					 AND date_part('year', v.date_vente) = ".$filtres->annee;
		
			if ($filtres->id_myProg_utilisateurs) {
				$sql  .= " AND v.id_user = ".$filtres->id_myProg_utilisateurs;
			} 
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result2 = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle 
			array_push($total,$result2);
			
			// PHASE 3 : CA SORTANT ( LE COMMERCIAL APPELLE POUR VENDRE ) + VOLUME SORTANT
			
			$sql  = "SELECT 
			         SUM(v.prix_vente) AS ca_sortant , 
			         COUNT(v.prix_vente) AS ca_sortant_vol 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'Sortant'
					 AND date_part('year', v.date_vente) = ".$filtres->annee;
		
			if ($filtres->id_myProg_utilisateurs) {
				$sql  .= " AND v.id_user = ".$filtres->id_myProg_utilisateurs;
			} 
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result3 = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle 
			array_push($total,$result3);
			
			// PHASE 4 : CA OPERATION( VENDU SUR CAMPAGNE SMS ET EMAILING ) + VOLUME OPERATION
			
			$sql  = "SELECT 
			         SUM(v.prix_vente) AS ca_operation , 
					 COUNT(v.prix_vente) AS ca_operation_vol 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'opération'
					 AND date_part('year', v.date_vente) = ".$filtres->annee;
		
			if ($filtres->id_myProg_utilisateurs) {
				$sql  .= " AND v.id_user = ".$filtres->id_myProg_utilisateurs;
			} 
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result4 = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle 
			array_push($total,$result4);
			
			
			// PHASE 5 : NB APPELS ENTRANT TRAITES
			  if($filtres->id_myProg_utilisateurs){
		
				$sql  = "SELECT 
				COUNT(id_crm_historique) AS count_calls 
				FROM crm_historique AS h 
				WHERE 1=1 
				AND date_part('year', h.calldate) = ".$filtres->annee."
				AND h.id_myProg_utilisateurs = ".$filtres->id_myProg_utilisateurs."
				";
							
			 }else{
				  $sql  = " SELECT COUNT(id_crm_historique) AS count_calls 
				FROM crm_historique AS h 
				WHERE 1=1 
				AND date_part('year', h.calldate) = ".$filtres->annee."
				";
			 }
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result5 = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle 
			array_push($total,$result5);
			
			// PHASE 6 : TAUX TRANSFORMATION ENTRANT  (NB VENTES / NB DEVIS)*100
			  if($filtres->id_myProg_utilisateurs){
		
				
				$sql  = " 
					SELECT ROUND((cast(nbVentes.nb_vente_entrant AS DECIMAL) / nbDevis.nb_devis)*100,2) AS txte FROM 
					(
					SELECT 
					 COUNT(v.prix_vente) AS nb_vente_entrant 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'Entrant'
					 AND date_part('year', v.date_vente) = ".$filtres->annee."
					 AND v.id_user = ".$filtres->id_myProg_utilisateurs."
					 ) AS nbVentes
					,
					 (
					SELECT 
					 COUNT(*) AS nb_devis
					 FROM crm_devis AS c 
					 WHERE 1=1 
					 AND date_part('year', c.date_creation) = ".$filtres->annee."
					 AND c.id_user = ".$filtres->id_myProg_utilisateurs."
					 ) AS nbDevis

				";
				
							
			 }else{
			$sql  = " 
					SELECT ROUND((cast(nbVentes.nb_vente_entrant AS DECIMAL) / nbDevis.nb_devis)*100,2) AS txte FROM 
					(
					SELECT 
					 COUNT(v.prix_vente) AS nb_vente_entrant 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'Entrant'
					 AND date_part('year', v.date_vente) = ".$filtres->annee."
					 ) AS nbVentes
					,
					 (
					SELECT 
					 COUNT(*) AS nb_devis
					 FROM crm_devis AS c 
					 WHERE 1=1 
					 AND date_part('year', c.date_creation) = ".$filtres->annee."
					 ) AS nbDevis

				";
			 }
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result6 = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle 
			array_push($total,$result6);
			
			
			// PHASE 7 : TAUX TRANSFORMATION SORTANT (NB VENTES / NB DEVIS)*100
			  if($filtres->id_myProg_utilisateurs){
		
				
				$sql  = " 
					SELECT ROUND((cast(nbVentes.nb_vente_sortant AS DECIMAL) / nbDevis.nb_devis)*100,2) AS txts FROM 
					(
					SELECT 
					 COUNT(v.prix_vente) AS nb_vente_sortant 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'Sortant'
					 AND date_part('year', v.date_vente) = ".$filtres->annee."
					 AND v.id_user = ".$filtres->id_myProg_utilisateurs."
					 ) AS nbVentes
					,
					 (
					SELECT 
					 COUNT(*) AS nb_devis
					 FROM crm_devis AS c 
					 WHERE 1=1 
					 AND date_part('year', c.date_creation) = ".$filtres->annee."
					 AND c.id_user = ".$filtres->id_myProg_utilisateurs."
					 ) AS nbDevis

				";
				
							
			 }else{
			$sql  = " 
					SELECT ROUND((cast(nbVentes.nb_vente_sortant AS DECIMAL) / nbDevis.nb_devis)*100,2) AS txts FROM 
					(
					SELECT 
					 COUNT(v.prix_vente) AS nb_vente_sortant 
					 FROM crm_vente AS v
					 LEFT JOIN crm_devis AS d ON v.id_crm_devis = d.id_crm_devis
					 WHERE 1=1 
					 AND d.type_vente = 'Sortant'
					 AND date_part('year', v.date_vente) = ".$filtres->annee."
					 ) AS nbVentes
					,
					 (
					SELECT 
					 COUNT(*) AS nb_devis
					 FROM crm_devis AS c 
					 WHERE 1=1 
					 AND date_part('year', c.date_creation) = ".$filtres->annee."
					 ) AS nbDevis

				";
			 }
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result7 = $req->Fetch(PDO::FETCH_ASSOC);
            $req->closeCursor();
			
			// On colle 
			array_push($total,$result7);
			
			
			
			// On envoye le tableau
            return $total;
			
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	/**
	 * Requête qui rapporte le ca total pour tous ou par personne
	 * Elle sert surtout dans les graphs camemberts pour la répartition dans les tableaux de bords commercial
	 * @return array | false
	 */
	
	public function GetRepartitionCaGraph($filtres) {
        try {
		
		
          $sql  = "
		  SELECT c.enseigne,count(v.prix_vente) AS count,sum(v.prix_vente) 
		  FROM crm_vente as v 
		  LEFT JOIN crm_comptes as c on v.id_comptes = c.id_crm_comptes 
		  WHERE date_part('year', v.date_vente) = ".$filtres->annee."";
		  
		 if ($filtres->mois) {
				$sql  .= " AND EXTRACT(MONTH FROM  v.date_vente) = ".$filtres->mois ; 
			} 	
		 
		 if ($filtres->id_myProg_utilisateurs) {
			$sql  .= " AND id_user = ".$filtres->id_myProg_utilisateurs;
		 }
			 
			$sql .= " GROUP BY c.enseigne ORDER BY count DESC ";
		
			
		
		 
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->FetchAll(PDO::FETCH_ASSOC);
            $req->closeCursor();
            return $result; 
			
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	
	
	/**
	 * Compte le nombre d'appels des 7 dernier jours par un utilisateur donné OU pour tous, dans la table crm_historique
	 * @return array_reverse | false
	 */
	public function GetCountCalls($filtres) {
        try {
			
			
		
		  if($filtres->id_myProg_utilisateurs){
	
			$sql  = " SELECT d.date, count(se.id_crm_historique) FROM (
							select to_char(date_trunc('day', (current_date - offs)), 'YYYY-MM-DD')
							AS date 
							FROM generate_series(0, 365, 1) 
							AS offs
							) d 
						LEFT OUTER JOIN (
							SELECT * FROM crm_historique
							WHERE id_myProg_utilisateurs = ".$filtres->id_myProg_utilisateurs."

						) se
						ON (d.date=to_char(date_trunc('day', se.calldate), 'YYYY-MM-DD')) 
						GROUP BY d.date
						ORDER BY d.date DESC
						LIMIT 7
						; ";
						
		 }else{
			  $sql  = " SELECT d.date, count(se.id_crm_historique) FROM (
							select to_char(date_trunc('day', (current_date - offs)), 'YYYY-MM-DD')
							AS date 
							FROM generate_series(0, 365, 1) 
							AS offs
							) d 
						LEFT OUTER JOIN (
							SELECT * FROM crm_historique

						) se
						ON (d.date=to_char(date_trunc('day', se.calldate), 'YYYY-MM-DD')) 
						GROUP BY d.date
						ORDER BY d.date DESC
						LIMIT 7
						; ";
		 }
			
			$req = $this->bdd->prepare($sql);
            $req->execute($tab_value);
            $result = $req->FetchAll(PDO::FETCH_ASSOC);
            $req->closeCursor();
            return  array_reverse($result); 
			
        } catch(Exception $e) {
			file_put_contents(DEBUG_SQL_LOG_FILE,$e->getMessage(),FILE_APPEND);
			return false;
        }
			
        
    }
	
	
	
	
 /* FIN DE LA CLASSE */
}
?>