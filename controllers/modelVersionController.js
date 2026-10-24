
const modelVersionService = require(
    "../services/modelVersionService"
  );
  
  
  // =========================================
  // LISTAR VERSIONES
  // =========================================
  
  async function getModelVersions(
    req,
    res,
    next
  ) {
    try {
      const items =
        await modelVersionService.listModelVersions();
  
      return res.json({
        ok: true,
        items
      });
    } catch (error) {
      next(error);
    }
  }
  
  
  // =========================================
  // ACTIVAR CANDIDATO
  // =========================================
  
  async function activateModelVersion(
    req,
    res,
    next
  ) {
    try {
      const modelVersionId =
        Number(req.params.id);
  
      const result =
        await modelVersionService.activateCandidateModel(
          modelVersionId
        );
  
      return res.json({
        ok: true,
        ...result
      });
    } catch (error) {
      next(error);
    }
  }
  
  
  // =========================================
  // DESCARTAR CANDIDATO
  // =========================================
  
  async function discardModelVersion(
    req,
    res,
    next
  ) {
    try {
      const modelVersionId =
        Number(req.params.id);
  
      const result =
        await modelVersionService.discardCandidateModel(
          modelVersionId
        );
  
      return res.json({
        ok: true,
        ...result
      });
    } catch (error) {
      next(error);
    }
  }
  
  
  module.exports = {
    getModelVersions,
    activateModelVersion,
    discardModelVersion
  };